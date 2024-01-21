package io.bhex.broker.server.grpc.server.service.kyc.impl;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.user.kyc.VideoKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.VideoKycVerifyResponse;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.domain.kyc.KycLevelEnum;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.grpc.server.service.kyc.AbstractKycVerify;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserKycApply;
import io.bhex.broker.server.model.UserVerify;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service
public class VipKycVerifyImpl extends AbstractKycVerify<VideoKycVerifyRequest, VideoKycVerifyResponse> {

    @Resource
    private UserService userService;

    @Value("${verify-captcha:true}")
    private Boolean verifyCaptcha;

    @Value("${global-notify-type:1}")
    private Integer globalNotifyType;

    @Override
    protected UserVerify preVerify(VideoKycVerifyRequest request) {
        UserVerify existUserVerify = getUserVerify(request.getHeader().getUserId(), false);
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
                    if (!existUserVerify.isVerifyPassed()) {
                        throw new BrokerException(BrokerErrorCode.KYC_SENIOR_VERIFY_UNDONE);
                    }
                    break;
                case VIP:
                    if (existUserVerify.isVerifyPassed()) {
                        throw new BrokerException(BrokerErrorCode.KYC_VIP_VERIFY_NONEED);
                    }
            }
        }

        if (StringUtils.isEmpty(existUserVerify.getDataSecret())) {
            existUserVerify.setDataSecret(UserVerify.createUserAESKey(existUserVerify.getUserId()));
            userVerifyMapper.update(existUserVerify);
        }

        // 视频认证（第三级认证）需要用户先完善手机号和邮箱信息
        User user = userService.getUser(request.getHeader().getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        if (verifyCaptcha && globalNotifyType == 2) {
            //仅手机时必须有手机
            if (StringUtils.isEmpty(user.getMobile())) {
                throw new BrokerException(BrokerErrorCode.NEED_BIND_MOBILE);
            }
        } else if (verifyCaptcha && globalNotifyType == 3) {
            //仅邮箱时，必须有邮箱
            if (StringUtils.isEmpty(user.getEmail())) {
                throw new BrokerException(BrokerErrorCode.NEED_BIND_EMAIL);
            }
        } else if (StringUtils.isEmpty(user.getEmail()) || StringUtils.isEmpty(user.getMobile())) {
            throw new BrokerException(BrokerErrorCode.KYC_MOBILE_EMAIL_REQUIRED);
        }

        return UserVerify.copyFrom(existUserVerify);
    }

    @Override
    protected VideoKycVerifyResponse doVerify(VideoKycVerifyRequest request, UserVerify userVerify) {
        Long userId = request.getHeader().getUserId();
        Long orgId = request.getHeader().getOrgId();

        // 对视频数据加密
        String encryptedFileKey = uploadEncryptedVideo(orgId, userId, request.getVideoUrl(), userVerify.getDataSecret());
        userVerify.setVideoUrl(encryptedFileKey);

        // 插入申请记录
        UserKycApply userKycApply = UserKycApply.builder()
                .id(sequenceGenerator.getLong())
                .kycLevel(KycLevelEnum.VIP.getValue())
                .userId(userId)
                .orgId(orgId)
                .videoUrl(userVerify.getVideoUrl())
                .dataSecret(userVerify.getDataSecret())
                .created(System.currentTimeMillis())
                .build();
        userKycApplyMapper.insertSelective(userKycApply);

        // 设置当前用户KYC申请记录ID
        userVerify.setKycApplyId(userKycApply.getId());

        // 保存VIP认证信息（三级认证）
        userVerify.setKycLevel(getLevel()); // 设置当前kyc级别
        userVerify.setUpdated(System.currentTimeMillis());
        userVerify.setVerifyStatus(UserVerifyStatus.UNDER_REVIEW.value());
        userVerifyMapper.update(userVerify);

        return VideoKycVerifyResponse.newBuilder()
                .setBizCode(OK_BIZ_CODE)
                .setBizMsg(OK_BIZ_MSG)
                .setKycApplyId(userKycApply.getId())
                .build();
    }

    @Override
    protected void postVerify(VideoKycVerifyRequest request, VideoKycVerifyResponse response, UserVerify userVerify) {

    }

    @Override
    public int getLevel() {
        return 30;
    }
}
