package io.bhex.broker.server.test;

import com.google.common.annotations.Beta;
import com.google.common.hash.Hashing;
import com.google.crypto.tink.subtle.Ed25519Sign;
import com.google.crypto.tink.subtle.Random;
import io.bhex.base.crypto.SignUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

@Slf4j
public class BrokerSignUtils extends SignUtil {

    /**
     * @param key
     * @param priKey
     * @param pubKey
     * @return
     */
    @Beta
    public static byte[] encryptPrivateKey(String key, byte[] priKey, byte[] pubKey) {
        byte[] keyBytes = Hashing.sha256().hashString(key, StandardCharsets.UTF_8).asBytes();
        priKey = byteMerger(priKey, pubKey);

        byte[] randBytes = Random.randBytes(16);
        IvParameterSpec iv = new IvParameterSpec(randBytes);
        SecretKeySpec skeySpec = new SecretKeySpec(keyBytes, "AES");

        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
            return byteMerger(randBytes, cipher.doFinal(priKey));
        } catch (Exception ex) {
            log.warn(String.format("Decrypt key fail! encrypted string %s", new String(priKey)));
            ex.printStackTrace();
        }
        return null;
    }

    private static byte[] byteMerger(byte[] byte1, byte[] byte2) {
        byte[] byte3 = new byte[byte1.length + byte2.length];
        System.arraycopy(byte1, 0, byte3, 0, byte1.length);
        System.arraycopy(byte2, 0, byte3, byte1.length, byte2.length);
        return byte3;
    }

    private static void initSignKey(String desc) {
        try {
            String encryptPrivateKey = RandomStringUtils.randomAlphanumeric(12);
            Ed25519Sign.KeyPair keyPair = Ed25519Sign.KeyPair.newKeyPair();
            byte[] privateKey = keyPair.getPrivateKey();
            byte[] publicKey = keyPair.getPublicKey();
            byte[] encrypt = encryptPrivateKey(encryptPrivateKey, privateKey, publicKey);

            String priKey = Base64.getEncoder().encodeToString(encrypt);
            String pubKey = Base64.getEncoder().encodeToString(publicKey);
            System.out.println(">>>>> " + desc + " <<<<<");
            System.out.println("encrypt_private_key_password: " + encryptPrivateKey);
            System.out.println("ENCRYPT_PRIVATEKEY_PASSWORD(base64): " + Base64.getEncoder().encodeToString(encryptPrivateKey.getBytes(StandardCharsets.UTF_8)));
            System.out.println(desc + " priKey: " + priKey);
            System.out.println(desc + " pubKey: " + pubKey);

            byte[] test = "hello".getBytes();

            // decrypt private key
            byte[] desBytes = decrypt(encryptPrivateKey, Base64.getDecoder().decode(priKey));

            // sign
            byte[] signBytes = sign(Arrays.copyOfRange(Objects.requireNonNull(desBytes), 0, 32), test);

            // verify
            try {
                verify(Base64.getDecoder().decode(pubKey), signBytes, test);
            } catch (GeneralSecurityException e) {
                e.printStackTrace();
            }
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        // 生成broker环境使用的signKey
        initSignKey("broker");
        // 生成blueHelix环境使用的signKey
        initSignKey("bh");
        // 生成rc环境使用的signKey
        initSignKey("rc");
    }
}
