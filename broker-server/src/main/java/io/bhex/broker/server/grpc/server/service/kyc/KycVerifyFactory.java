package io.bhex.broker.server.grpc.server.service.kyc;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.user.kyc.*;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.kyc.impl.*;
import io.bhex.broker.server.model.BrokerKycConfig;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class KycVerifyFactory {

    @Resource
    private BasicKycVerifyImpl basicKycVerify;

    @Resource
    @Qualifier("SeniorKycVerify")
    private SeniorKycVerifyImpl seniorKycVerify;

    @Resource
    @Qualifier("SeniorFaceKycVerify")
    private SeniorFaceKycVerifyImpl seniorFaceKycVerify;

    @Resource
    private SeniorFaceResultKycVerifyImpl seniorFaceResultKycVerify;

    @Resource
    private VipKycVerifyImpl vipKycVerify;

    @Resource
    private BasicService basicService;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    public BasicKycVerifyResponse basicVerify(BasicKycVerifyRequest request) {
        return basicKycVerify.verify(request);
    }

    public PhotoKycVerifyResponse seniorVerify(PhotoKycVerifyRequest request) {
        UserVerify userVerify = userVerifyMapper.getByUserId(request.getHeader().getUserId());
        if (userVerify == null) {
            throw new BrokerException(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE);
        }

        if (isNeedFaceCompare(userVerify)) {
            return seniorFaceKycVerify.verify(request);
        } else {
            return seniorKycVerify.verify(request);
        }
    }

    public FaceKycVerifyResponse seniorFaceResultVerify(FaceKycVerifyRequest request) {
        return seniorFaceResultKycVerify.verify(request);
    }

    public VideoKycVerifyResponse vipVerify(VideoKycVerifyRequest request) {
        return vipKycVerify.verify(request);
    }

    private boolean isNeedFaceCompare(UserVerify userVerify) {
        BrokerKycConfig config = basicService.getBrokerKycConfig(userVerify.getOrgId(), userVerify.getNationality());
        if (config == null) {
            config = BrokerKycConfig.newDefaultInstance(userVerify.getOrgId());
        }
        return config.getSecondKycLevel() != 20;
    }
}
