package io.bhex.broker.server.grpc.server.service.kyc.impl;

import com.google.common.base.Strings;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.user.kyc.PhotoKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.PhotoKycVerifyResponse;
import io.bhex.broker.grpc.user.kyc.SdkPrepareInfo;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.domain.kyc.tencent.*;
import io.bhex.broker.server.grpc.server.service.kyc.tencent.WebankKycException;
import io.bhex.broker.server.grpc.server.service.kyc.tencent.WebankKycService;
import io.bhex.broker.server.model.UserVerify;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service("SeniorFaceKycVerify")
public class SeniorFaceKycVerifyImpl extends SeniorKycVerifyImpl {

    @Resource
    private WebankKycService webankKycService;

    @Override
    protected UserVerify preVerify(PhotoKycVerifyRequest request) {
        // 人脸识别需要校验身份证正反面照片
        if (StringUtils.isEmpty(request.getCardFrontUrl()) || StringUtils.isEmpty(request.getCardBackUrl())) {
            throw new BrokerException(BrokerErrorCode.KYC_PHOTO_PARAM_INVALID);
        }

        // 查询是否已经存在认证记录
        UserVerify existUserVerify = checkExistUserVeirfy(request.getHeader().getUserId());
        return UserVerify.copyFrom(existUserVerify);
    }

    @Override
    protected PhotoKycVerifyResponse doVerify(PhotoKycVerifyRequest request, UserVerify userVerify) {
        PhotoKycVerifyResponse response = super.doVerify(request, userVerify);

        Long userId = request.getHeader().getUserId();
        Long orgId = request.getHeader().getOrgId();
        try {
            // 尝试解密（如果有加密字段的话）
            UserVerify decryptedUserVerify = UserVerify.decrypt(userVerify);

//            // 先走身份证OCR识别，校验身份证照片和基础认证中的实名信息是否一致
//            IDCardOcrResponse ocrResponse = webankKycService.idCardOcr(
//                    response.getKycApplyId(), userId, orgId, request.getCardFrontUrl());
//            checkOcrResponse(ocrResponse, decryptedUserVerify);

            // 走腾讯人脸识别，获取faceId，返回给客户端
            NonceTicket nonceTicket = webankKycService.getNonceTicket(userId, orgId);

            // 获取faceid时，不传比对源照片参数（腾讯接口目前规则是：自带比对源目前不会校验身份信息）
            GetFaceIdResponse faceIdResponse = webankKycService.getFaceId(response.getKycApplyId(), userId, orgId,
                    decryptedUserVerify.getCardNo(), getUserVerifyChineseName(decryptedUserVerify),
                    null, nonceTicket);

            // 获取faceid和SDK登录的订单号要用同一个
            SDKLoginPrepareResponse sdkResp = webankKycService.sdkLoginPrepare(request.getHeader(), request.getAppType(),
                    faceIdResponse.getResult().getOrderNo(), nonceTicket);
            SdkPrepareInfo prepareInfo = toSdkPrepareInfo(sdkResp);
            prepareInfo = prepareInfo.toBuilder().setFaceId(faceIdResponse.getResult().getFaceId()).build();

            return PhotoKycVerifyResponse.newBuilder()
                    .setBizCode(OK_BIZ_CODE)
                    .setBizMsg(OK_BIZ_MSG)
                    .setFaceCompare(true)
                    .setSdkPrepareInfo(prepareInfo)
                    .setKycApplyId(response.getKycApplyId())
                    .build();
        } catch (WebankKycException e) {
            userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(), 0L, e.getBizMsg());
            return PhotoKycVerifyResponse.newBuilder()
                    .setBizCode(e.getBizCode())
                    .setBizMsg(e.getBizMsg())
                    .build();
        }
    }

    @Override
    public int getLevel() {
        return 25;
    }

//    /**
//     * 检查ocr识别出来的身份信息是否和基础认证的身份信息一致
//     */
//    private void checkOcrResponse(IDCardOcrResponse ocrResponse, UserVerify userVerify) {
//        IDCardOcrObject ocrObject = ocrResponse.getResult();
//        if (ocrObject == null) {
//            log.error("checkOcrResponse error for ocrObject is null.");
//            throw new BrokerException(BrokerErrorCode.KYC_IDENTITY_PHOTO_NOT_MATCH);
//        }
//
//        if (!StringUtils.equals(ocrObject.getName(), userVerify.getFirstName()) ||
//                !StringUtils.equals(ocrObject.getIdcard(), userVerify.getCardNo())) {
//            log.error("checkOcrResponse match failed. [OCR] name: {} idCard: {} [UserVerify] firstName: {} cardNo: {}",
//                    ocrObject.getName(), ocrObject.getIdcard(), userVerify.getFirstName(), userVerify.getCardNo());
//            throw new BrokerException(BrokerErrorCode.KYC_IDENTITY_PHOTO_NOT_MATCH);
//        }
//    }

    private static SdkPrepareInfo toSdkPrepareInfo(SDKLoginPrepareResponse sdkResp) {
        return SdkPrepareInfo.newBuilder()
                .setAppId(sdkResp.getWebankAppId())
                .setLicense(sdkResp.getLicense())
                .setNonce(sdkResp.getNonce())
                .setVersion(sdkResp.getVersion())
                .setOrderNo(sdkResp.getOrderNo())
                .setSign(sdkResp.getSign())
                .setUserId(sdkResp.getUserId())
                .build();
    }

    private static String getUserVerifyChineseName(UserVerify userVerify) {
        return String.format("%s%s", Strings.nullToEmpty(userVerify.getFirstName()),
                Strings.nullToEmpty(userVerify.getSecondName()));
    }

    protected int getInitVerifyStatus() {
        // 人脸识别设置为未审核
        return UserVerifyStatus.NONE.value();
    }
}
