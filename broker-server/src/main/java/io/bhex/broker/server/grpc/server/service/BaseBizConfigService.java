package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import static io.bhex.broker.server.domain.BaseConfigConstants.*;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.domain.WithdrawCheckResult;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.BaseSymbolConfigInfo;
import io.bhex.broker.server.model.BaseTokenConfigInfo;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BaseBizConfigService {

    @Resource
    private BaseConfigService baseConfigService;
    @Resource
    private BaseSymbolConfigService baseSymbolConfigService;
    @Resource
    private BaseTokenConfigService baseTokenConfigService;

    public BaseConfigInfo getOneConfig(long orgId, String group, String key) {
        return getOneConfig(orgId, group, key, null);
    }

    private BaseConfigInfo getOneConfig(long orgId, String group, String key, String language) {
        BaseConfigInfo baseConfigInfo = baseConfigService.getOneConfig(orgId, group, key, language, true);
        if (baseConfigInfo == null || !baseConfigInfo.getIsOpen()) {
            return null;
        }
        return baseConfigInfo;
    }

//    public List<BaseConfigInfo> getConfigsByGroupAndKey(String group, String key) {
//        return baseConfigService.getConfigs(0L, Lists.newArrayList(group), Lists.newArrayList(key), null,  0, 0);
//    }


    public List<BaseConfigInfo> getConfigsByGroupsAndKey(long orgId, List<String> groups, String key) {
        return baseConfigService.getConfigs(orgId, groups, Lists.newArrayList(key), null,  0, 0);
    }

    public List<BaseConfigInfo> getConfigsByGroup(long orgId, String group) {
        return baseConfigService.getConfigs(orgId, Lists.newArrayList(group), null, null,  0, 0);
    }
//
//    private List<BaseConfigInfo> getConfigsByGroup(long orgId, String group, String language) {
//        return getConfigs(orgId, Arrays.asList(group), new ArrayList<>(), language);
//    }
//
//    private List<BaseConfigInfo> getConfigs(long orgId, String group, List<String> keys) {
//        return getConfigs(orgId, Arrays.asList(group), keys, null);
//    }
//
//    private List<BaseConfigInfo> getConfigs(long orgId, List<String> groups, List<String> keys, String language) {
//        List<BaseConfigInfo> configs = baseConfigService.getConfigs(orgId, groups, keys, language);
//        if (CollectionUtils.isEmpty(configs)) {
//            return new ArrayList<>();
//        }
//        return configs.stream().filter(c -> c.getIsOpen()).collect(Collectors.toList());
//    }


    public SwitchStatus getConfigSwitchStatus(long orgId, String group, String key) {
        return getConfigSwitchStatus(orgId, group, key, null);
    }

    private SwitchStatus getConfigSwitchStatus(long orgId, String group, String key, String language) {
        if (BROKER_INI_GROUP_LIST.contains(group)) { //在缓存中取值
            String cacheKey = buildBaseConfigKey(orgId, group, key, language);
            if (!brokerConfigMap.containsKey(cacheKey)) {
                return SwitchStatus.builder().existed(false).open(false).build();
            } else {
                ConfigData configData = brokerConfigMap.get(cacheKey);
                return SwitchStatus.builder().existed(true).open(configData.value.toLowerCase().equals("true")).build();
            }
        }

        SwitchStatus switchStatus;
        BaseConfigInfo configInfo = getOneConfig(orgId, group, key, language);
        if (configInfo == null) {
            switchStatus = SwitchStatus.builder().existed(false).open(false).build();
        } else {
            switchStatus = SwitchStatus.builder().existed(true).open(configInfo.getConfValue().trim().toLowerCase().equals("true")).build();
        }
        if (switchStatus.isOpen() && BROKER_INI_GROUP_LIST.contains(group)) {
            log.info("brokerConfigMap:{}", brokerConfigMap);
            log.error("cacheKey:{} not found in cache", buildBaseConfigKey(orgId, group, key, language));
        }
        return switchStatus;
    }

    public List<BaseSymbolConfigInfo> getSymbolConfigsByGroupAndKey(String group, String key) {
        return baseSymbolConfigService.getSymbolConfigsByGroupAndKey(group, key);
    }

//    private BaseSymbolConfigInfo getOneSymbolConfig(long orgId, String symbol, String group, String key) {
//        return getOneSymbolConfig(orgId, symbol, group, key, null);
//    }
//
//    private BaseSymbolConfigInfo getOneSymbolConfig(long orgId, String symbol, String group, String key, String language) {
//        BaseSymbolConfigInfo baseConfigInfo = baseSymbolConfigService.getOneSymbolConfig(orgId, symbol, group, key, language, true);
//        if (!baseConfigInfo.getIsOpen()) {
//            return null;
//        }
//        return baseConfigInfo;
//    }
//
//    private List<BaseSymbolConfigInfo> getSymbolConfigsByGroup(long orgId, List<String> symbols, String group) {
//        return getSymbolConfigs(orgId, symbols, Arrays.asList(group), new ArrayList<>(), null);
//    }
//
//    private List<BaseSymbolConfigInfo> getSymbolConfigs(long orgId, List<String> symbols, String group, List<String> keys) {
//        return getSymbolConfigs(orgId, symbols, Arrays.asList(group), keys, null);
//    }
//
//    private List<BaseSymbolConfigInfo> getSymbolConfigs(long orgId, List<String> symbols, List<String> groups, List<String> keys, String language) {
//        List<BaseSymbolConfigInfo> configs = baseSymbolConfigService.getSymbolConfigs(orgId, symbols, groups, keys, language);
//        if (CollectionUtils.isEmpty(configs)) {
//            return new ArrayList<>();
//        }
//        return configs.stream().filter(c -> c.getIsOpen()).collect(Collectors.toList());
//    }
//
//    public SwitchStatus getSymbolConfigSwitchStatus(long orgId, String symbol, String group, String key) {
//        return getSymbolConfigSwitchStatus(orgId, symbol, group, key, null);
//    }
//
//    public SwitchStatus getSymbolConfigSwitchStatus(long orgId, String symbol, String group, String key, String language) {
//        BaseSymbolConfigInfo configInfo = getOneSymbolConfig(orgId, symbol, group, key, language);
//        if (configInfo == null) {
//            return SwitchStatus.builder().existed(false).open(false).build();
//        }
//        return SwitchStatus.builder().existed(true).open(configInfo.getConfValue().toLowerCase().equals("true")).build();
//    }
//
//
//    public BaseTokenConfigInfo getOneTokenConfig(long orgId, String token, String group, String key) {
//        return getOneTokenConfig(orgId, token, group, key, null);
//    }
//
//    public BaseTokenConfigInfo getOneTokenConfig(long orgId, String token, String group, String key, String language) {
//        BaseTokenConfigInfo baseConfigInfo = baseTokenConfigService.getOneTokenConfig(orgId, token, group, key, language, true);
//        if (!baseConfigInfo.getIsOpen()) {
//            return null;
//        }
//        return baseConfigInfo;
//    }
//
//    public List<BaseTokenConfigInfo> getTokenConfigsByGroup(long orgId, List<String> tokens, String group) {
//        return getTokenConfigs(orgId, tokens, Arrays.asList(group), new ArrayList<>(), null);
//    }
//
//    public List<BaseTokenConfigInfo> getTokenConfigs(long orgId, List<String> tokens, String group, List<String> keys) {
//        return getTokenConfigs(orgId, tokens, Arrays.asList(group), keys, null);
//    }
//
//    private List<BaseTokenConfigInfo> getTokenConfigs(long orgId, List<String> tokens, List<String> groups, List<String> keys, String language) {
//        List<BaseTokenConfigInfo> configs = baseTokenConfigService.getTokenConfigs(orgId, tokens, groups, keys, language);
//        if (CollectionUtils.isEmpty(configs)) {
//            return new ArrayList<>();
//        }
//        return configs.stream().filter(c -> c.getIsOpen()).collect(Collectors.toList());
//    }
//
//    public SwitchStatus getTokenConfigSwitchStatus(long orgId, String token, String group, String key) {
//        return getTokenConfigSwitchStatus(orgId, token, group, key, null);
//    }
//
//    public SwitchStatus getTokenConfigSwitchStatus(long orgId, String token, String group, String key, String language) {
//        BaseTokenConfigInfo configInfo = getOneTokenConfig(orgId, token, group, key, language);
//        if (configInfo == null) {
//            return SwitchStatus.builder().existed(false).open(false).build();
//        }
//        return SwitchStatus.builder().existed(true).open(configInfo.getConfValue().toLowerCase().equals("true")).build();
//    }






    private static final List<String> BROKER_INI_GROUP_LIST = Lists.newArrayList(
            WHOLE_SITE_CONTROL_SWITCH_GROUP, FROZEN_USER_OTC_TRADE_GROUP,
            FROZEN_USER_OPTION_TRADE_GROUP, FROZEN_USER_FUTURE_TRADE_GROUP,
            FROZEN_USER_OTC_TRADE_GROUP, FROZEN_USER_BONUS_TRADE_GROUP,
            FORCE_AUDIT_USER_WITHDRAW_GROUP, FROZEN_USER_COIN_TRADE_GROUP,
            FROZEN_USER_API_COIN_TRADE_GROUP, USER_LEVEL_CONFIG_GROUP, APPPUSH_CONFIG_GROUP,
            ALLOW_TRADING_SYMBOLS_GROUP,FROZEN_USER_MARGIN_TRADE_GROUP, DISABLE_USER_TRADING_SYMBOLS_GROUP,
            "saas.broker.switch"
    );


    private static final List<String> BROKER_SYMBOL_INI_GROUP_LIST = Lists.newArrayList(

    );



    private static final List<String> BROKER_TOKEN_INI_GROUP_LIST = Lists.newArrayList(

    );

    @Data
    @Builder(builderClassName = "Builder", toBuilder = true)
    public static class ConfigData {
        private String key;

        private String value;

        private String extraValue;
    }

    private static Map<String, ConfigData> brokerConfigMap = ImmutableMap.of();

    private static Map<String, ConfigData> brokerSymbolConfigMap = ImmutableMap.of();

    private static Map<String, ConfigData> brokerTokenConfigMap = ImmutableMap.of();

    @PostConstruct
    @Scheduled(cron = "0/10 * * * * ?")
    public void initConfig() {

        initBrokerBaseConfig();

        //initBrokerBaseSymbolConfig();

        //initBrokerBaseTokenConfig();

    }


    private void initBrokerBaseConfig() {
        Map<String, ConfigData> tempConfigMap = Maps.newHashMap();

        List<BaseConfigInfo> baseConfigs = baseConfigService.getConfigs(0L, BROKER_INI_GROUP_LIST, null, null, 0, 0);
        for (BaseConfigInfo c : baseConfigs) {
            String key = buildBaseConfigKey(c.getOrgId(), c.getConfGroup(), c.getConfKey(), c.getLanguage());
            tempConfigMap.put(key, ConfigData.builder().key(c.getConfKey()).value(c.getConfValue()).extraValue(c.getExtraValue()).build());
        }
        brokerConfigMap = ImmutableMap.copyOf(tempConfigMap);
    }


//    private void initBrokerBaseSymbolConfig() {
//
//        Map<String, ConfigData> tempConfigMap = Maps.newHashMap();
//        List<BaseSymbolConfigInfo> brokerBaseSymbolConfigs = baseSymbolConfigService.getSymbolConfigs(0L, BROKER_SYMBOL_INI_GROUP_LIST, null, null);
//        for (BaseSymbolConfigInfo c : brokerBaseSymbolConfigs) {
//            String key = buildBaseSymbolConfigKey(c.getOrgId(), c.getSymbol(), c.getConfGroup(), c.getConfKey(), c.getLanguage());
//            tempConfigMap.put(key, ConfigData.builder().key(c.getConfKey()).value(c.getConfValue()).extraValue(c.getExtraValue()).build());
//        }
//        brokerSymbolConfigMap = ImmutableMap.copyOf(tempConfigMap);
//    }


//    private void initBrokerBaseTokenConfig() {
//        Map<String, ConfigData> tempConfigMap = Maps.newHashMap();
//        List<BaseTokenConfigInfo> baseConfigs = baseTokenConfigService.getTokenConfigs(0L, BROKER_TOKEN_INI_GROUP_LIST, null, null);
//        for (BaseTokenConfigInfo c : baseConfigs) {
//            String key = buildBaseTokenConfigKey(c.getOrgId(), c.getToken(), c.getConfGroup(), c.getConfKey(), c.getLanguage());
//            tempConfigMap.put(key, ConfigData.builder().key(c.getConfKey()).value(c.getConfValue()).extraValue(c.getExtraValue()).build());
//        }
//        brokerTokenConfigMap = ImmutableMap.copyOf(tempConfigMap);
//    }
//
//
//    private String buildBaseSymbolConfigKey(long orgId, String symbol, String group, String key, String language) {
//        return buildBaseConfigKey(orgId, group, key, language) + "-" + symbol;
//    }

    private String buildBaseTokenConfigKey(long orgId, String token, String group, String key, String language) {
        return buildBaseConfigKey(orgId, group, key, language) + "-" + token;
    }


    private String buildBaseConfigKey(long orgId, String group, String key, String language) {
        return orgId + "-" + group + "-" + key + "-" + Strings.nullToEmpty(language);
    }



    public ConfigData getBrokerBaseConfig(long orgId, String group, String key, String language) {
        String configKey = buildBaseConfigKey(orgId, group, key, language);
        return brokerConfigMap.get(configKey);
    }
}
