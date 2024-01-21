package io.bhex.broker.server.grpc.interceptor;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;
import io.bhex.broker.server.model.BrokerOrgConfig;
import io.bhex.broker.server.util.OrgConfigUtil;
import io.grpc.*;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RouteAuthCredentials implements CallCredentials {

    private final String routeKey;
    private final Long orgId;
    private static final String BEARER = "bhop_bearer_token:";
    private static final Metadata.Key<String> API_KEY_META = Metadata.Key.of("ApiKey", Metadata.ASCII_STRING_MARSHALLER);
    private static final int SECRET_KEY_CACHE_INVALID_SECONDS = 5 * 60;
    /**
     * 认证刷新时间（gateway失效时间差1分钟）
     */
    private static final long AUTH_REFRESH_TIME = 4 * 60 * 1000L;
    private static Cache<Long, BrokerAuth> brokerAuthCache = CacheBuilder.newBuilder()
        .expireAfterWrite(SECRET_KEY_CACHE_INVALID_SECONDS, TimeUnit.SECONDS)
        .build();
    
    public RouteAuthCredentials(String routeKey,Long orgId) {
        this.routeKey = routeKey;
        this.orgId = orgId;
    }
    
    @Override
    public void applyRequestMetadata(MethodDescriptor<?, ?> method, Attributes attrs, Executor appExecutor, MetadataApplier applier) {
        appExecutor.execute(() -> {
            try {
                //检查发送认证
                BrokerAuth brokerAuth = brokerAuthCache.getIfPresent(orgId);
                if(brokerAuth == null || brokerAuth.refreshTime <= System.currentTimeMillis()){
                    BrokerOrgConfig brokerOrgConfig = OrgConfigUtil.getBrokerOrgConfig(orgId);
                    if(brokerOrgConfig == null){
                        log.warn("unknown orgId:{}", orgId);
                        applier.fail(Status.INTERNAL.withDescription("unknown orgId:" + orgId));
                        return;
                    }
                    brokerAuth = makeBrokerAuth(brokerOrgConfig);
                    brokerAuthCache.put(orgId,brokerAuth);
                }
                Metadata headers = new Metadata();
                Metadata.Key<String> authKey = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
                headers.put(API_KEY_META, brokerAuth.getApiKey());
                headers.put(authKey, brokerAuth.authData);
                Metadata.Key<String> rKey = Metadata.Key.of("route-channel", Metadata.ASCII_STRING_MARSHALLER);
                headers.put(rKey, routeKey);
                applier.apply(headers);
            } catch (Throwable e) {
                applier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
        });
    }

    /**
     * 供其他工程使用
     * @param orgId
     * @return
     */
    public static BrokerAuth getBrokerAuthByOrgId(Long orgId){
        BrokerAuth brokerAuth = brokerAuthCache.getIfPresent(orgId);
        if(brokerAuth == null || brokerAuth.refreshTime <= System.currentTimeMillis()){
            BrokerOrgConfig brokerOrgConfig = OrgConfigUtil.getBrokerOrgConfig(orgId);
            if(brokerOrgConfig == null){
                log.warn("unknown orgId:{}", orgId);
                return null;
            }
            brokerAuth = makeBrokerAuth(brokerOrgConfig);
            brokerAuthCache.put(orgId,brokerAuth);
        }
        return brokerAuth;
    }
    
    @Override
    public void thisUsesUnstableApi() {
    
    }

    private static BrokerAuth makeBrokerAuth(BrokerOrgConfig brokerOrgConfig){
        long currentTime = System.currentTimeMillis();
        String secretKey = brokerOrgConfig.getSecretKey();
        String signature = Hashing.hmacSha256(secretKey.getBytes()).hashString(BEARER + "#" + secretKey + "#" + currentTime, Charsets.UTF_8).toString();
        String authData = BEARER + signature + "#" + currentTime;
        return BrokerAuth.builder()
                .apiKey(brokerOrgConfig.getApiKey())
                .refreshTime(currentTime + AUTH_REFRESH_TIME)
                .authData(authData).build();
    }

    /**
     * broker认证缓存
     */
    @Data
    @Builder
    public static class BrokerAuth {
        /**
         * 刷新时间（加签时间+4分钟）
         */
        private long refreshTime;
        /**
         * 认证串
         */
        private String authData;

        private String apiKey;
    }
}
