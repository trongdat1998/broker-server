package io.bhex.broker.server.grpc.server.service.kyc;

import com.google.common.net.MediaType;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.objectstorage.ObjectStorage;
import io.bhex.broker.common.objectstorage.ObjectStorageUtil;
import io.bhex.broker.common.util.AESCipher;
import io.bhex.broker.common.util.FileUtil;
import io.bhex.broker.server.domain.KycLevelGrade;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.FileStorageService;
import io.bhex.broker.server.grpc.server.service.UserVerifyService;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.UserKycApplyMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.util.UUID;

@Slf4j
@Service
public abstract class AbstractKycVerify<R, P> implements KycVerify<R, P> {

    public static final Integer DATA_ENCRYPTED = 1;

    public static final String OK_BIZ_CODE = "0";
    public static final String OK_BIZ_MSG = "success";

    public static final String AWS_FILE_KEY_PREFIX_PLAIN = "plain";
    public static final String AWS_FILE_KEY_PREFIX_ENCRYPT = "encrypt";

    @Resource
    protected UserVerifyMapper userVerifyMapper;

    @Resource
    protected UserKycApplyMapper userKycApplyMapper;

    @Resource
    protected ISequenceGenerator sequenceGenerator;

    @Resource
    protected BasicService basicService;

    @Resource
    protected FileStorageService fileStorageService;

    @Resource
    protected UserVerifyService userVerifyService;

    @Override
    public KycLevelGrade getGrade() {
        return KycLevelGrade.fromKycLevel(getLevel());
    }

    protected abstract UserVerify preVerify(R request);

    protected abstract P doVerify(R request, UserVerify userVerify);

    protected abstract void postVerify(R request, P response, UserVerify userVerify);

    @Override
    public P verify(R request) {
        UserVerify userVerify = null;
        try {
            userVerify = preVerify(request);
            P response = doVerify(request, userVerify);
            postVerify(request, response, userVerify);
            return response;
        } catch (BrokerException e) {
            userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(),
                    0L, String.format("Get broker error: %s", e.getCode()));
            throw e;
        } catch (Exception e) {
            userVerifyService.updateVerifyResult(userVerify, UserVerifyStatus.REFUSED.value(),
                    0L, "system internal error");
            log.error(String.format("KycLevelGrade %s verify error", getGrade()), e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }

    /**
     * 根据用户ID获取数据库中的UserVerify实体对象
     *
     * @param userId 用户ID
     * @param needDecrypt 是否需要解密
     * @return UserVerify实体对象
     */
    protected UserVerify getUserVerify(Long userId, boolean needDecrypt) {
        UserVerify userVerify = userVerifyMapper.getByUserId(userId);
        if (userVerify == null) {
            return null;
        }

        if (needDecrypt && DATA_ENCRYPTED.equals(userVerify.getDataEncrypt())) {
            return UserVerify.decrypt(userVerify);
        } else {
            return userVerify;
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    protected String uploadEncryptedImage(Long orgId, Long userId, String plainFileKey, String dataSecret) {
        return uploadEncryptedFile(orgId, userId, plainFileKey, dataSecret, MediaType.ANY_IMAGE_TYPE);
    }

    @SuppressWarnings("UnstableApiUsage")
    protected String uploadEncryptedVideo(Long orgId, Long userId, String plainFileKey, String dataSecret) {
        return uploadEncryptedFile(orgId, userId, plainFileKey, dataSecret, MediaType.ANY_VIDEO_TYPE);
    }

    @SuppressWarnings("UnstableApiUsage")
    private String uploadEncryptedFile(Long orgId, Long userId, String plainFileKey,
                                         String dataSecret, MediaType mediaType) {
        if (!plainFileKey.contains(AWS_FILE_KEY_PREFIX_PLAIN)) {
            throw new IllegalArgumentException(String.format("plainFileKey %s must contain '%s'",
                    plainFileKey, AWS_FILE_KEY_PREFIX_PLAIN));
        }

        String fileSuffix = FileUtil.getFileSuffix(plainFileKey, "");

        File encryptedFile = fileStorageService.encryptFile(plainFileKey, dataSecret);
        String encryptedFileKey;
        try {
            // 文件路经格式为 encrypt/{orgId}/{userId}/sha256_file_data.xxx
            encryptedFileKey = String.format("%s/%s/%s/%s", AWS_FILE_KEY_PREFIX_ENCRYPT, orgId, userId,
                    ObjectStorageUtil.sha256FileName(encryptedFile, fileSuffix));
            log.info("plainFileKey: {} enctypedFileKey: {}", plainFileKey, encryptedFileKey);
        } catch (IOException e) {
            log.error("uploadEncryptedFile error.", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
        fileStorageService.uploadFile(encryptedFileKey, mediaType, encryptedFile);
        log.info("uploadEncryptedFile: upload new encrypt file ok. enctypedFileKey: {}", encryptedFileKey);
        return encryptedFileKey;
    }
}
