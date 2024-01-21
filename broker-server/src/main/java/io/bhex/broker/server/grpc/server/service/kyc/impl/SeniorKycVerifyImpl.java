package io.bhex.broker.server.grpc.server.service.kyc.impl;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.user.kyc.PhotoKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.PhotoKycVerifyResponse;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.kyc.AbstractKycVerify;
import io.bhex.broker.server.grpc.server.service.kyc.tencent.WebankKycService;
import io.bhex.broker.server.model.UserKycApply;
import io.bhex.broker.server.model.UserVerify;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service("SeniorKycVerify")
public class SeniorKycVerifyImpl extends AbstractKycVerify<PhotoKycVerifyRequest, PhotoKycVerifyResponse> {

    @Resource
    private BasicService basicService;

    @Resource
    private WebankKycService webankKycService;

    @Override
    protected UserVerify preVerify(PhotoKycVerifyRequest request) {
        // 非人脸识别需要校验证件的正面和手持照片
        if (StringUtils.isEmpty(request.getCardFrontUrl()) || StringUtils.isEmpty(request.getCardHandUrl())) {
            throw new BrokerException(BrokerErrorCode.KYC_PHOTO_PARAM_INVALID);
        }

        // 查询是否已经存在认证记录
        UserVerify existUserVerify = checkExistUserVeirfy(request.getHeader().getUserId());
        return UserVerify.copyFrom(existUserVerify);
    }

    @Override
    protected PhotoKycVerifyResponse doVerify(PhotoKycVerifyRequest request, UserVerify userVerify) {
        Long userId = request.getHeader().getUserId();
        Long orgId = request.getHeader().getOrgId();

        // 存储加密过的正面照片信息
        String cardFrontFileKey = request.getCardFrontUrl();
        if (StringUtils.isNotEmpty(cardFrontFileKey)) {
            String encryptedCardFrontFileKey = uploadEncryptedImage(orgId, userId, cardFrontFileKey, userVerify.getDataSecret());
            userVerify.setCardFrontUrl(encryptedCardFrontFileKey);
        }

        // 存储加密过的背面照片信息
        String cardBackFileKey = request.getCardBackUrl();
        if (StringUtils.isNotEmpty(cardBackFileKey)) {
            String encryptedCardBackFileKey = uploadEncryptedImage(orgId, userId, cardBackFileKey, userVerify.getDataSecret());
            userVerify.setCardBackUrl(encryptedCardBackFileKey);
        }

        // 存储加密过的手持照片信息
        String cardHandFileKey = request.getCardHandUrl();
        if (StringUtils.isNotEmpty(cardHandFileKey)) {
            String encryptedCardHandFileKey = uploadEncryptedImage(orgId, userId, cardHandFileKey, userVerify.getDataSecret());
            userVerify.setCardHandUrl(encryptedCardHandFileKey);
        }

        // 设置当前的KYC级别
        userVerify.setKycLevel(getLevel());

        // 插入申请记录
        UserKycApply userKycApply = UserKycApply.builder()
                .id(sequenceGenerator.getLong())
                .kycLevel(userVerify.getKycLevel())
                .userId(userVerify.getUserId())
                .orgId(userVerify.getOrgId())
                .cardFrontUrl(userVerify.getCardFrontUrl())
                .cardBackUrl(userVerify.getCardBackUrl())
                .cardHandUrl(userVerify.getCardHandUrl())
                .dataSecret(userVerify.getDataSecret())
                .created(System.currentTimeMillis())
                .build();
        userKycApplyMapper.insertSelective(userKycApply);

        // 设置当前用户KYC申请记录ID

        userVerify.setKycApplyId(userKycApply.getId());

        // 存储高级认证KYC信息(二级)
        userVerify.setUpdated(System.currentTimeMillis());
        userVerify.setVerifyStatus(getInitVerifyStatus());
        userVerifyMapper.update(userVerify);

        return PhotoKycVerifyResponse.newBuilder()
                .setBizCode(OK_BIZ_CODE)
                .setBizMsg(OK_BIZ_MSG)
                .setKycApplyId(userKycApply.getId())
                .setFaceCompare(false)
                .build();
    }

    @Override
    protected void postVerify(PhotoKycVerifyRequest request, PhotoKycVerifyResponse response, UserVerify userVerify) {
        // do nothing
    }

    @Override
    public int getLevel() {
        return 20;
    }

    protected UserVerify checkExistUserVeirfy(Long userId) {
        UserVerify existUserVerify = getUserVerify(userId, false);
        if (existUserVerify == null) {
            throw new BrokerException(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE);
        } else {
            switch (existUserVerify.getGrade()) {
                case BASIC:
                    if (!existUserVerify.isVerifyPassed()) {
                        throw new BrokerException(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE);
                    }
                    break;
                case SENIOR:
                    if (existUserVerify.isVerifyPassed()) {
                        throw new BrokerException(BrokerErrorCode.KYC_SENIOR_VERIFY_NONEED);
                    }
                    break;
                default:
                    throw new BrokerException(BrokerErrorCode.KYC_SENIOR_VERIFY_NONEED);
            }
        }

        if (StringUtils.isEmpty(existUserVerify.getDataSecret())) {
            existUserVerify.setDataSecret(UserVerify.createUserAESKey(existUserVerify.getUserId()));
            userVerifyMapper.update(existUserVerify);
        }

        return existUserVerify;
    }

    protected int getInitVerifyStatus() {
        // 非人脸识别设置为审核中
        return UserVerifyStatus.UNDER_REVIEW.value();
    }
}
