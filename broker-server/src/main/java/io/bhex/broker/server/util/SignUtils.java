/**********************************
 * @项目名称: broker-parent
 * @文件名称: SignUtils
 * @Date 2018/10/8
 * @Author liweiwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/

package io.bhex.broker.server.util;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import io.bhex.base.crypto.SignUtil;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.model.Broker;
import io.jsonwebtoken.impl.TextCodec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SignUtils {

    /**
     *
     */
    private static final String ENV_KEY = "ENCRYPT_PRIVATEKEY_PASSWORD";
    /**
     * Encrypt password for private key
     */
    private static final String ENCRYPT_PASSWORD = System.getenv(ENV_KEY);
    /**
     * Sign key
     */
    private static ImmutableMap<Long, Broker> brokerKeys = ImmutableMap.of();

    @Autowired
    private BrokerMapper brokerMapper;

    @PostConstruct
    @Scheduled(cron = "0 0/5 * * * ?")
    public void initSingKey() {
        List<Broker> brokerList = brokerMapper.selectAll();
        Map<Long, Broker> tmpBrokerMap = Maps.newHashMap();
        for (Broker broker : brokerList) {
            if (broker.getStatus() != 1) {
                continue;
            }
            broker.setDecryptKey(SignUtil.decrypt(ENCRYPT_PASSWORD, Base64.getDecoder().decode(broker.getPrivateKey())));
            tmpBrokerMap.put(broker.getOrgId(), broker);
        }
        brokerKeys = ImmutableMap.copyOf(tmpBrokerMap);
    }

    /**
     * Sign by SHA256withECDSA
     *
     * @param byteArray
     * @param orgId
     * @return
     * @throws Exception
     */
    public String sign(byte[] byteArray, long orgId) {
        Broker broker = brokerKeys.get(orgId);
        try {
            return SignUtil.signWithVersion(broker.getKeyVersion(), broker.getDecryptKey(), byteArray);
        } catch (Exception e) {
            log.error("bluehelix sign with error, orgId:{} broker:{}", orgId, JsonUtil.defaultGson().toJson(broker), e);
            throw e;
        }
    }

    /**
     * 加密算法
     */
    private static final String AES_ALGORITHM = "AES";

    /**
     * 加密算法/加密模式/填充类型
     * 本例采用AES加密，ECB加密模式，PKCS5Padding填充
     */
    private static final String AES_CIPHER_MODE = "AES/ECB/PKCS5Padding";

    public static String encryptDataWithAES(String key, String data) throws Exception {
        return encryptDataWithAES(key, data.getBytes(Charsets.UTF_8));
    }

    public static String encryptDataWithAES(String key, byte[] data) throws Exception {
        byte[] keyBytes = Hashing.sha256().hashString(key, Charsets.UTF_8).asBytes();
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, AES_ALGORITHM);
        Cipher cipher = Cipher.getInstance(AES_CIPHER_MODE);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        byte[] cipherTextBytes = cipher.doFinal(data);
        return TextCodec.BASE64.encode(cipherTextBytes);
    }

    public static String decryptDataWithAES(String key, String data) throws Exception {
        return decryptDataWithAES(key, TextCodec.BASE64.decode(data));
    }

    public static String decryptDataWithAES(String key, byte[] data) throws Exception {
        byte[] keyBytes = Hashing.sha256().hashString(key, Charsets.UTF_8).asBytes();
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, AES_ALGORITHM);
        Cipher cipher = Cipher.getInstance(AES_CIPHER_MODE);
        cipher.init(Cipher.DECRYPT_MODE, keySpec);
        byte[] cipherTextBytes = cipher.doFinal(data);
        return new String(cipherTextBytes, Charsets.UTF_8);
    }

}
