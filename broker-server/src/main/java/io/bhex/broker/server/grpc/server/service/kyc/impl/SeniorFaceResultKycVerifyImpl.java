package io.bhex.broker.server.grpc.server.service.kyc.impl;

import com.google.common.net.MediaType;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.objectstorage.ObjectStorageUtil;
import io.bhex.broker.common.util.AESCipher;
import io.bhex.broker.grpc.user.kyc.FaceKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.FaceKycVerifyResponse;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.domain.kyc.tencent.FaceCompareObject;
import io.bhex.broker.server.domain.kyc.tencent.IDCardOcrObject;
import io.bhex.broker.server.domain.kyc.tencent.IDCardOcrResponse;
import io.bhex.broker.server.domain.kyc.tencent.QueryResultResponse;
import io.bhex.broker.server.grpc.server.service.FileStorageService;
import io.bhex.broker.server.grpc.server.service.PushDataService;
import io.bhex.broker.server.grpc.server.service.kyc.AbstractKycVerify;
import io.bhex.broker.server.grpc.server.service.kyc.tencent.WebankKycException;
import io.bhex.broker.server.grpc.server.service.kyc.tencent.WebankKycService;
import io.bhex.broker.server.model.UserVerify;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class SeniorFaceResultKycVerifyImpl extends AbstractKycVerify<FaceKycVerifyRequest, FaceKycVerifyResponse> {

    @Resource
    private WebankKycService webankKycService;

    @Resource
    private FileStorageService fileStorageService;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private PushDataService pushDataService;

    @Override
    protected UserVerify preVerify(FaceKycVerifyRequest request) {
        UserVerify existUserVerify = getUserVerify(request.getHeader().getUserId(), false);
        if (existUserVerify == null) {
            throw new BrokerException(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE);
        }

        return UserVerify.copyFrom(existUserVerify);
    }

    @Override
    protected FaceKycVerifyResponse doVerify(FaceKycVerifyRequest request, UserVerify userVerify) {
        Long orgId = request.getHeader().getOrgId();
        Long userId = request.getHeader().getUserId();
        try {
            QueryResultResponse queryResult = webankKycService.queryFaceCompareResult(userId, orgId, request.getOrderNo(), "1");

            // 调用步骤
            // 1. 先将审核状态设置为审核中
            // 2. 然后异步更新审核结果
            userVerify.setUpdated(System.currentTimeMillis());
            userVerify.setVerifyStatus(UserVerifyStatus.UNDER_REVIEW.value());
            userVerifyMapper.update(userVerify);

            // 异步更新审核结果
            CompletableFuture.runAsync(() -> updateVerifyResult(queryResult, userVerify), taskExecutor);

            return FaceKycVerifyResponse.newBuilder()
                    .setBizCode(OK_BIZ_CODE)
                    .setBizMsg(OK_BIZ_MSG)
                    .build();
        } catch (WebankKycException e) {
            // 人脸验证不成功
            userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(), 0L, e.getBizMsg());

            return FaceKycVerifyResponse.newBuilder()
                    .setBizCode(e.getBizCode())
                    .setBizMsg(e.getBizMsg())
                    .build();
        }
    }

    @Override
    protected void postVerify(FaceKycVerifyRequest request, FaceKycVerifyResponse response, UserVerify userVerify) {
        // do nothing
        //KYC通过数据推给需要的客户
//        pushDataService.userKycMessage(userVerify.getOrgId(), userVerify.getUserId());
    }

    @Override
    public int getLevel() {
        return 25;
    }

    @SuppressWarnings("UnstableApiUsage")
    private void trySaveFaceCompareMedia(UserVerify userVerify, FaceCompareObject result) {
        try {
            // 尝试保存人脸识别照片
            if (StringUtils.isNotEmpty(result.getPhoto())) {
                byte[] photoData = Base64.decodeBase64(result.getPhoto());

                String fileKey;
                if (DATA_ENCRYPTED.equals(userVerify.getDataEncrypt())) {
                    fileKey = uploadEncryptFileBytes(userVerify.getOrgId(), userVerify.getUserId(), photoData,
                            userVerify.getDataSecret(), MediaType.ANY_IMAGE_TYPE, "jpg");
                } else {
                    fileKey = uploadPlainFileBytes(userVerify.getOrgId(), userVerify.getUserId(),
                            photoData, MediaType.ANY_IMAGE_TYPE, "jpg");
                }
                log.info("save face result photo: {} success", fileKey);
                userVerify.setFacePhotoUrl(fileKey);
            }

            // 尝试保存人脸识别视频
            if (StringUtils.isNotEmpty(result.getVideo())) {
                byte[] videoData = Base64.decodeBase64(result.getVideo());

                String fileKey;
                if (DATA_ENCRYPTED.equals(userVerify.getDataEncrypt())) {
                    fileKey = uploadEncryptFileBytes(userVerify.getOrgId(), userVerify.getUserId(), videoData,
                            userVerify.getDataSecret(), MediaType.ANY_VIDEO_TYPE, "mp4");
                } else {
                    fileKey = uploadPlainFileBytes(userVerify.getOrgId(), userVerify.getUserId(),
                            videoData, MediaType.ANY_VIDEO_TYPE, "mp4");
                }
                log.info("save face result video: {} success", fileKey);
                userVerify.setVideoUrl(fileKey);
            }
        } catch (Throwable e) {
            log.error("trySaveFaceCompareMedia error", e);
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private String uploadPlainFileBytes(Long orgId, Long userId, byte[] fileData, MediaType mediaType,
                                        String suffix) throws IOException {
        String fileKey = String.format("%s/%s/%s/%s", AWS_FILE_KEY_PREFIX_PLAIN, orgId,
                userId, ObjectStorageUtil.sha256FileName(fileData, suffix));
        fileStorageService.uploadFileBytes(fileKey, mediaType, fileData);
        return fileKey;
    }

    @SuppressWarnings("UnstableApiUsage")
    private String uploadEncryptFileBytes(Long orgId, Long userId, byte[] fileData, String secret, MediaType mediaType,
                                          String suffix) throws IOException {
        try (ByteArrayInputStream input = new ByteArrayInputStream(fileData);
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            AESCipher.encryptStream(input, output, secret);

            byte[] encryptedData = output.toByteArray();
            String fileKey = String.format("%s/%s/%s/%s", AWS_FILE_KEY_PREFIX_ENCRYPT, orgId, userId,
                    ObjectStorageUtil.sha256FileName(encryptedData, suffix));
            fileStorageService.uploadFileBytes(fileKey, mediaType, encryptedData);
            return fileKey;
        }
    }

    /**
     * 检查ocr识别出来的身份信息是否和基础认证的身份信息一致
     */
    private void checkOcrResponse(IDCardOcrResponse ocrResponse, UserVerify userVerify) {
        IDCardOcrObject ocrObject = ocrResponse.getResult();
        if (ocrObject == null) {
            log.error("checkOcrResponse error for ocrObject is null.");
            throw new BrokerException(BrokerErrorCode.KYC_IDENTITY_PHOTO_NOT_MATCH);
        }

        if (!StringUtils.equals(ocrObject.getName(), userVerify.getFirstName()) ||
                !StringUtils.equalsIgnoreCase(ocrObject.getIdcard(), userVerify.getCardNo())) {
            log.error("checkOcrResponse match failed. [OCR] name: {} idCard: {} [UserVerify] firstName: {} cardNo: {}",
                    ocrObject.getName(), ocrObject.getIdcard(), userVerify.getFirstName(), userVerify.getCardNo());
            throw new BrokerException(BrokerErrorCode.KYC_IDENTITY_PHOTO_NOT_MATCH);
        }
    }

    private void updateVerifyResult(QueryResultResponse queryResult, UserVerify userVerify) {
        trySaveFaceCompareMedia(userVerify, queryResult.getResult());

        UserVerify decryptedUserVerify = UserVerify.decrypt(userVerify);
        File cardFrontFile;
        if (StringUtils.isEmpty(userVerify.getCardFrontUrl())) {
            userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(),
                    0L, "card front photo is empty");
            return;
        } else {
            try {
                if (userVerify.getCardFrontUrl().startsWith("encrypt")) {
                    cardFrontFile = fileStorageService.decryptFile(userVerify.getCardFrontUrl(), userVerify.getDataSecret());
                } else {
                    cardFrontFile = fileStorageService.donwloadFile(userVerify.getCardFrontUrl());
                }
            } catch (Exception e) {
                log.error(String.format("asyncUpdateVerifyResult: download: %s error", userVerify.getCardFrontUrl()), e);
                cardFrontFile = null;
            }

            if (cardFrontFile == null) {
                userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(),
                        0L, "download card front photo error");
                return;
            }
        }

        String idCardBase64 = fileToBase64String(cardFrontFile);
        if (idCardBase64 == null) {
            userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(),
                    0L, "front photo base64 error");
            return;
        }

        try {
            // 走身份证OCR识别，校验身份证照片和基础认证中的实名信息是否一致
            IDCardOcrResponse ocrResponse = webankKycService.idCardOcr(decryptedUserVerify.getKycApplyId(),
                    decryptedUserVerify.getUserId(), decryptedUserVerify.getOrgId(), idCardBase64);
            checkOcrResponse(ocrResponse, decryptedUserVerify);
        } catch (Exception e) {
            log.error(String.format("userId: %s OCR error.", decryptedUserVerify.getUserId()), e);
            userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(),
                    0L, e.getMessage());
            return;
        }

        // 人脸验证成功，更新审核状态为通过
        userVerify.setVerifyStatus(UserVerifyStatus.PASSED.value());
        userVerify.setVerifyReasonId(0L);

        // 更新数据库状态
       boolean updateFlag = userVerifyService.updateVerifyResultPassed(userVerify);
       if(updateFlag){
           //推送KYC
           pushDataService.userKycMessage(userVerify.getOrgId(), userVerify.getUserId());
       }

    }

    private static String fileToBase64String(File file) {
        try {
            byte[] bytes = Files.readAllBytes(file.toPath());
            return Base64.encodeBase64String(bytes);
        } catch (IOException e) {
            log.error(String.format("fileToBase64 file: %s error", file.getPath()), e);
            return null;
        }
    }
}
