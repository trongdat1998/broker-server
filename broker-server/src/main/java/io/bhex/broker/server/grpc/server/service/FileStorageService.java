package io.bhex.broker.server.grpc.server.service;

import com.google.common.net.MediaType;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.objectstorage.ObjectStorage;
import io.bhex.broker.common.util.AESCipher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.util.UUID;

@Slf4j
@Service
public class FileStorageService {

    @Resource
    private ObjectStorage awsObjectStorage;

    @SuppressWarnings("UnstableApiUsage")
    public void uploadFileBytes(String fileKey, MediaType mediaType, byte[] data) {
        awsObjectStorage.uploadObject(fileKey, mediaType, data);
    }

    @SuppressWarnings("UnstableApiUsage")
    public void uploadFile(String fileKey, MediaType mediaType, File file) {
        awsObjectStorage.uploadObject(fileKey, mediaType, file);
    }

    public File donwloadFile(String fileKey) {
        return donwloadFile(fileKey, null);
    }

    public File donwloadFile(String fileKey, String suffix) {
        File tmpFile;
        try {
            tmpFile = File.createTempFile(UUID.randomUUID().toString(), suffix);
        } catch (IOException e) {
            log.error("donwloadFile error.", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
        awsObjectStorage.downloadObject(fileKey, tmpFile);
        return tmpFile;
    }

    /**
     * 根据plainFileKey从存储服务器下载文件，并加密
     *
     * @param plainFileKey 未加密的文件存储路径KEY
     * @param dataSecret 密钥
     * @return 返回加密过的文件File
     */
    public File encryptFile(String plainFileKey, String dataSecret) {
        File plainFile, encryptedFile;
        try {
            plainFile = File.createTempFile(UUID.randomUUID().toString(), null);
            encryptedFile = File.createTempFile(UUID.randomUUID().toString(), null);
            log.info("plainFile: {} encryptedFile: {}", plainFile.getAbsolutePath(), encryptedFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("encryptFile error.", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        // 下载未加密文件
        awsObjectStorage.downloadObject(plainFileKey, plainFile);

        try (InputStream input = new FileInputStream(plainFile);
             OutputStream output = new FileOutputStream(encryptedFile)) {
            AESCipher.encryptStream(input, output, dataSecret);
            return encryptedFile;
        } catch (IOException e) {
            log.error("encryptFile error.", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }

    /**
     * 根据encryptFileKey从存储服务器下载文件，并解密
     *
     * @param encryptFileKey 加密的文件存储路径KEY
     * @param dataSecret 密钥
     * @return 返回解密过的文件File
     */
    public File decryptFile(String encryptFileKey, String dataSecret) {
        File plainFile, encryptedFile;
        try {
            encryptedFile = File.createTempFile(UUID.randomUUID().toString(), null);
            plainFile = File.createTempFile(UUID.randomUUID().toString(), null);
            log.info("encryptedFile: {}， plainFile: {} ", encryptedFile.getAbsolutePath(), plainFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("decryptFile error.", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        // 下载加密文件
        awsObjectStorage.downloadObject(encryptFileKey, encryptedFile);

        try (InputStream input = new FileInputStream(encryptedFile);
             OutputStream output = new FileOutputStream(plainFile)) {
            AESCipher.decryptStream(input, output, dataSecret);
            return plainFile;
        } catch (IOException e) {
            log.error("decryptFile error.", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }
}
