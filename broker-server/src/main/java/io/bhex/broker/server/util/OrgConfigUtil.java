package io.bhex.broker.server.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import io.bhex.broker.server.model.BrokerOrgConfig;
import io.bhex.broker.server.primary.mapper.BrokerOrgConfigMapper;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.ApplicationContext;

/**
 * @author wangsc
 * @description org配置工具
 * @date 2020-06-01 16:26
 */
@Slf4j
public class OrgConfigUtil {

    /**
     * 暂定最高缓存1小时,定时任务半小时获取一次全部配置
     */
    private static final int API_KEY_CACHE_INVALID_MINUTES = 60;
    private static ApplicationContext applicationContext;

    private static ImmutableSet<Long> orgSets = ImmutableSet.of();

    private static LoadingCache<Long, BrokerOrgConfig> orgConfigCache = CacheBuilder.newBuilder()
        .expireAfterAccess(API_KEY_CACHE_INVALID_MINUTES, TimeUnit.MINUTES)
        .build(new CacheLoader<Long, BrokerOrgConfig>() {
            @Override
            public BrokerOrgConfig load(Long orgId) {
                return applicationContext.getBean(BrokerOrgConfigMapper.class)
                    .getBrokerOrgConfig(orgId);
            }
        });

    public static void init(ApplicationContext applicationContext) {
        OrgConfigUtil.applicationContext = applicationContext;
        //首次加载全部配置用于定时任务的轮询
        refreshAllOrgConfigs();
        //执行定时任务(~~~等全部走gateway的时候在使用spring的定时任务~~~)
        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("RefreshOrgConfig_%d").daemon(true).build());
        scheduler.scheduleAtFixedRate(OrgConfigUtil::refreshAllOrgConfigs,30L,30L,TimeUnit.MINUTES);
    }

    public static BrokerOrgConfig getBrokerOrgConfig(Long orgId) {
        try {
            return orgConfigCache.get(orgId);
        } catch (Exception e) {
            log.warn("get org config error! {} {}", orgId, e);
            return null;
        }
    }

    /**
     * 用于定时任务的刷新（每30分钟刷新一次吧）
     */
    public static void refreshAllOrgConfigs() {
        try {
            List<BrokerOrgConfig> brokerOrgConfigs = applicationContext
                .getBean(BrokerOrgConfigMapper.class).getAllBrokerOrgConfigs();
            orgSets = ImmutableSet.copyOf(
                brokerOrgConfigs.stream().map(BrokerOrgConfig::getOrgId)
                    .collect(Collectors.toSet()));
            brokerOrgConfigs.forEach(brokerOrgConfig -> orgConfigCache
                .put(brokerOrgConfig.getOrgId(), brokerOrgConfig));
            ImmutableSet.copyOf(
                    brokerOrgConfigs.stream().map(BrokerOrgConfig::getOrgId)
                    .collect(
                        Collectors.toSet()));
            log.info("currently supported org id: {}",orgSets);
        } catch (Exception e) {
            log.warn("get all org config error! {}", e.getMessage());
        }
    }

    /**
     * 获取全部的orgId用于定时任务循环执行
     *
     * @return
     */
    public static Set<Long> getOrgSets() {
        return orgSets;
    }



    /**
     * broker认证缓存
     */
    @Data
    @Builder
    public static class BrokerApiAuth {
        private String apiKey;
        /**
         * 刷新时间（加签时间+4分钟）
         */
        private long refreshTime;
        /**
         * 认证串
         */
        private String authData;
    }
}
