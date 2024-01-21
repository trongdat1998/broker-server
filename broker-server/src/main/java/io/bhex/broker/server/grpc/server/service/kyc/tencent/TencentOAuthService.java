package io.bhex.broker.server.grpc.server.service.kyc.tencent;

import com.google.gson.Gson;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.kyc.tencent.*;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.model.BrokerKycConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Service
public class TencentOAuthService {

    private static final String TICKET_TYPE_NONCE = "NONCE";
    private static final String TICKET_TYPE_SIGN = "SIGN";

    private static final String VERSION = "1.0.0";

    // 腾讯人脸识别ACCESS_TOKEN缓存Key
    public static final String TENCENT_FACE_ACCESS_TOKEN_KEY = "tencent_face_access_token_key:";
    // 腾讯人脸识别SIGN TICKET缓存Key
    public static final String TENCENT_FACE_SIGN_TICKET_KEY = "tencent_face_sign_ticket_key:";

    @Resource
    private IdascSAO idascSAO;

    @Resource
    private BasicService basicService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    public BrokerKycConfig getBrokerKycConfig(Long orgId) {
        BrokerKycConfig config = basicService.getBrokerKycConfig(orgId);
        if (config == null) {
            throw new BrokerException(BrokerErrorCode.KYC_BROKER_CONFIG_NOT_FOUND);
        }
        return config;
    }

    public String getVersion() {
        return VERSION;
    }

    public String getAccessToken(Long orgId) {
        GetAccessTokenResponse accessTokenResponse = getAccessTokenResponseFromRedis(orgId);
        if (accessTokenResponse == null) {
            accessTokenResponse = requestAccessToken(orgId);
            if (accessTokenResponse != null) {
                setAccessTokenResponseToRedis(orgId, accessTokenResponse);
            }
        }
        return accessTokenResponse == null ? null : accessTokenResponse.getAccessToken();
    }

    public String getSignTicket(Long orgId) {
        TicketObject signTicketResponse = getSignTicketFromRedis(orgId);
        if (signTicketResponse == null) {
            signTicketResponse = requestSignTicket(orgId);
            if (signTicketResponse != null) {
                setSignTicketToRedis(orgId, signTicketResponse);
            }
        }
        return signTicketResponse == null ? null : signTicketResponse.getValue();
    }

    public String getNonceTicket(Long userId, Long orgId) {
        BrokerKycConfig brokerKycConfig = getBrokerKycConfig(orgId);

        GetApiTicketRequest request = new GetApiTicketRequest();
        request.setAppId(brokerKycConfig.getWebankAppId());
        request.setType(TICKET_TYPE_NONCE);
        request.setVersion(getVersion());
        request.setUserId(String.valueOf(userId));
        request.setAccessToken(getAccessToken(orgId));
        log.info("get nonce ticket , request: {}", request);
        try {
            GetApiTicketResponse ticketResponse = idascSAO.getApiTicket(request);
            TicketObject ticketObject = ticketResponse.getTickets().get(0);
            log.info("get nonce ticket success, {}", ticketObject);
            return ticketObject.getValue();
        } catch (Exception e) {
            if (e instanceof BrokerException) {
                throw e;
            }
            log.error("get nonce ticket error", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }

    private GetAccessTokenResponse requestAccessToken(Long orgId) {
        BrokerKycConfig brokerKycConfig = getBrokerKycConfig(orgId);

        GetAccessTokenRequest request = new GetAccessTokenRequest();
        request.setAppId(brokerKycConfig.getWebankAppId());
        request.setSecret(brokerKycConfig.getWebankAppSecret());
        request.setGrantType("client_credential");
        request.setVersion(getVersion());
        try {
            GetAccessTokenResponse response = idascSAO.getAccessToken(request);
            if (!response.getCode().equals("0")) {
                log.warn("request access token fail, code: {}, msg: {}", response.getCode(), response.getMsg());
                return null;
            }
            log.info("request access token success, accessToken: {}, expireIn: {}",
                    response.getAccessToken(), response.getExpireIn());
            return response;
        } catch (Exception e) {
            if (e instanceof BrokerException) {
                throw e;
            }
            log.error("request access token error", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }

    private TicketObject requestSignTicket(Long orgId) {
        BrokerKycConfig brokerKycConfig = getBrokerKycConfig(orgId);

        GetApiTicketRequest request = new GetApiTicketRequest();
        request.setAppId(brokerKycConfig.getWebankAppId());
        request.setType(TICKET_TYPE_SIGN);
        request.setVersion(getVersion());
        request.setAccessToken(getAccessToken(orgId));
        try {
            GetApiTicketResponse response = idascSAO.getApiTicket(request);
            if (!response.getCode().equals("0")) {
                log.warn("request sign ticket fail, code: {}, msg: {}", response.getCode(), response.getMsg());
                return null;
            }
            TicketObject ticketObject = response.getTickets().get(0);
            log.info("request sign ticket success, signTicket: {}, expireIn: {}", ticketObject.getValue(), ticketObject.getExpireIn());
            return ticketObject;
        } catch (Exception e) {
            if (e instanceof BrokerException) {
                throw e;
            }
            log.error("request sign ticket error", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }

    public static String createNonce() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private GetAccessTokenResponse getAccessTokenResponseFromRedis(Long orgId) {
        String key = String.format("%s%s", TENCENT_FACE_ACCESS_TOKEN_KEY, orgId);
        String value = redisTemplate.opsForValue().get(key);
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        Gson gson = new Gson();
        try {
            return gson.fromJson(value, GetAccessTokenResponse.class);
        } catch (Throwable e) {
            log.error("getAccessTokenResponseFromRedis: parse json error: " + value);
            return null;
        }
    }

    private void setAccessTokenResponseToRedis(Long orgId, GetAccessTokenResponse accessToken) {
        String key = String.format("%s%s", TENCENT_FACE_ACCESS_TOKEN_KEY, orgId);
        String value = JsonUtil.defaultGson().toJson(accessToken);
        int timeout = getRedisKeyTimeout(accessToken.getExpireIn());
        redisTemplate.opsForValue().set(key, value, timeout, TimeUnit.SECONDS);
        log.info("setAccessTokenResponseToRedis ok. key: {} value: {} timeout: {}", key, value, timeout);
    }

    private void setSignTicketToRedis(Long orgId, TicketObject signTicket) {
        String key = String.format("%s%s", TENCENT_FACE_SIGN_TICKET_KEY, orgId);
        String value = JsonUtil.defaultGson().toJson(signTicket);

        int timeout = getRedisKeyTimeout(signTicket.getExpireIn());
        redisTemplate.opsForValue().set(key, value, timeout, TimeUnit.SECONDS);
        log.info("setSignTicketToRedis ok. key: {} value: {} timeout: {}", key, value, timeout);
    }

    private TicketObject getSignTicketFromRedis(Long orgId) {
        String key = String.format("%s%s", TENCENT_FACE_SIGN_TICKET_KEY, orgId);
        String value = redisTemplate.opsForValue().get(key);
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        Gson gson = new Gson();
        try {
            return gson.fromJson(value, TicketObject.class);
        } catch (Throwable e) {
            log.error("getSignTicketFromRedis: parse json error: " + value);
            return null;
        }
    }

    /**
     * 返回redis里key的过期时间(单位为秒)，默认比腾讯服务器返回的expireIn少10秒
     */
    private static int getRedisKeyTimeout(String expireIn) {
        try {
            int expireInInt = Integer.parseInt(expireIn);
            int timeout =  expireInInt - 10;
            return (timeout > 0) ? timeout : expireInInt;
        } catch (NumberFormatException e) {
            log.error("getExpireIn error for:" + expireIn);
            return 0;
        }
    }
}
