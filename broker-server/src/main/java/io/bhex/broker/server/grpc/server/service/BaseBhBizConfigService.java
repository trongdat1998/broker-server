package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.bhex.base.common.*;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.grpc.client.service.GrpcBaseConfigService;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BaseBhBizConfigService {

    @Resource
    private GrpcBaseConfigService grpcBaseConfigService;

    public BaseConfigInfo getOneConfig(long orgId, String group, String key) {
        return getOneConfig(orgId, group, key, null);
    }

    public BaseConfigInfo getOneConfig(long orgId, String group, String key, String language) {
        BaseConfig grpcBaseConfig = grpcBaseConfigService.getOneBhBaseConfig(GetOneBaseConfigRequest.newBuilder()
                .setOrgId(orgId).setGroup(group).setKey(key).setLanguage(Strings.nullToEmpty(language))
                .build());
        if (grpcBaseConfig.getId() == 0 || !grpcBaseConfig.getIsOpen()) {
            return null;
        }

        BaseConfigInfo baseConfigInfo = new BaseConfigInfo();
        BeanCopyUtils.copyPropertiesIgnoreNull(grpcBaseConfig, baseConfigInfo);
        baseConfigInfo.setConfKey(grpcBaseConfig.getKey());
        baseConfigInfo.setConfValue(grpcBaseConfig.getValue());
        return baseConfigInfo;
    }

//    public List<BaseConfigInfo> getConfigsByGroup(long orgId, String group) {
//        return getConfigs(orgId, group, new ArrayList<>(), null);
//    }
//
//    public List<BaseConfigInfo> getConfigsByGroup(long orgId, String group, String language) {
//        return getConfigs(orgId, group, new ArrayList<>(), language);
//    }
//
    public List<BaseConfigInfo> getConfigs(long orgId, String group, List<String> keys, String language) {
        List<BaseConfig> grpcConfigs = grpcBaseConfigService.getBhBaseConfigs(GetBaseConfigsRequest.newBuilder()
                .setOrgId(orgId)
                .setGroup(group)
                .addAllKey(CollectionUtils.isNotEmpty(keys) ? keys : new ArrayList<>())
                .setLanguage(Strings.nullToEmpty(language))
                .build()).getBaseConfigList();
        if (CollectionUtils.isEmpty(grpcConfigs)) {
            return new ArrayList<>();
        }
        List<BaseConfigInfo> configs = grpcConfigs.stream()
                .filter(c -> c.getIsOpen())
                .map(c -> {
                    BaseConfigInfo baseConfigInfo = new BaseConfigInfo();
                    BeanCopyUtils.copyPropertiesIgnoreNull(c, baseConfigInfo);
                    baseConfigInfo.setConfKey(c.getKey());
                    baseConfigInfo.setConfValue(c.getValue());
                    return baseConfigInfo;
                })
                .collect(Collectors.toList());
        return configs;
    }




    public SwitchStatus getConfigSwitchStatus(long orgId, String group, String key) {
        return getConfigSwitchStatus(orgId, group, key, null);
    }

    public SwitchStatus getConfigSwitchStatus(long orgId, String group, String key, String language) {
        BaseConfigInfo configInfo = getOneConfig(orgId, group, key, language);
        if (configInfo == null) {
            return SwitchStatus.builder().existed(false).open(false).build();
        }
        return SwitchStatus.builder().existed(true).open(configInfo.getConfValue().toLowerCase().equals("true")).build();
    }

//    public BaseSymbolConfigInfo getOneSymbolConfig(long orgId, String symbol, String group, String key) {
//        return getOneSymbolConfig(orgId, symbol, group, key, null);
//    }
//
//
//    public BaseSymbolConfigInfo getOneSymbolConfig(long orgId, String symbol, String group, String key, String language) {
//        BaseSymbolConfig grpcBaseConfig = grpcBaseConfigService.getOneBhBaseSymbolConfig(GetOneBaseSymbolConfigRequest.newBuilder()
//                .setOrgId(orgId)
//                .setGroup(group)
//                .setKey(key)
//                .setLanguage(Strings.nullToEmpty(language))
//                .setSymbol(Strings.nullToEmpty(symbol))
//                .build());
//        if (grpcBaseConfig.getId() == 0 || !grpcBaseConfig.getIsOpen()) {
//            return null;
//        }
//
//        BaseSymbolConfigInfo baseConfigInfo = new BaseSymbolConfigInfo();
//        BeanCopyUtils.copyPropertiesIgnoreNull(grpcBaseConfig, baseConfigInfo);
//        baseConfigInfo.setConfKey(grpcBaseConfig.getKey());
//        baseConfigInfo.setConfValue(grpcBaseConfig.getValue());
//        return baseConfigInfo;
//    }
//
//    public List<BaseSymbolConfigInfo> getSymbolConfigsByGroup(long orgId, List<String> symbols, String group) {
//        return getSymbolConfigs(orgId, symbols, group, new ArrayList<>(), null);
//    }
//
//    public List<BaseSymbolConfigInfo> getSymbolConfigs(long orgId, List<String> symbols, String group, List<String> keys, String language) {
//        List<BaseSymbolConfig> grpcConfigs = grpcBaseConfigService.getBhBaseSymbolConfigs(GetBaseSymbolConfigsRequest.newBuilder()
//                .setOrgId(orgId)
//                .setGroup(group)
//                .addAllKey(CollectionUtils.isNotEmpty(keys) ? keys : new ArrayList<>())
//                .setLanguage(Strings.nullToEmpty(language))
//                .addAllSymbol(CollectionUtils.isNotEmpty(symbols) ? symbols : new ArrayList<>())
//                .build()).getBaseConfigList();
//        if (CollectionUtils.isEmpty(grpcConfigs)) {
//            return new ArrayList<>();
//        }
//        List<BaseSymbolConfigInfo> configs = grpcConfigs.stream()
//                .filter(c -> c.getIsOpen())
//                .map(c -> {
//                    BaseSymbolConfigInfo baseConfigInfo = new BaseSymbolConfigInfo();
//                    BeanCopyUtils.copyPropertiesIgnoreNull(c, baseConfigInfo);
//                    baseConfigInfo.setConfKey(c.getKey());
//                    baseConfigInfo.setConfValue(c.getValue());
//                    return baseConfigInfo;
//                })
//                .collect(Collectors.toList());
//        return configs;
//    }
//
//
//
//    public SwitchStatus getSymbolConfigSwitchStatus(long orgId, String symbol, String group, String key) {
//        return getSymbolConfigSwitchStatus(orgId, symbol, group, key, null);
//    }
//
//    public SwitchStatus getSymbolConfigSwitchStatus(long orgId, String symbol, String group, String key, String language) {
//        BaseSymbolConfigInfo configInfo = getOneSymbolConfig(orgId, symbol, group, key);
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
//        BaseTokenConfig grpcBaseConfig = grpcBaseConfigService.getOneBhBaseTokenConfig(GetOneBaseTokenConfigRequest.newBuilder()
//                .setOrgId(orgId)
//                .setGroup(group)
//                .setKey(key)
//                .setLanguage(Strings.nullToEmpty(language))
//                .setToken(Strings.nullToEmpty(token))
//                .build());
//        if (grpcBaseConfig.getId() == 0 || !grpcBaseConfig.getIsOpen()) {
//            return null;
//        }
//
//        BaseTokenConfigInfo baseConfigInfo = new BaseTokenConfigInfo();
//        BeanCopyUtils.copyPropertiesIgnoreNull(grpcBaseConfig, baseConfigInfo);
//        baseConfigInfo.setConfKey(grpcBaseConfig.getKey());
//        baseConfigInfo.setConfValue(grpcBaseConfig.getValue());
//        return baseConfigInfo;
//    }
//
//    public List<BaseTokenConfigInfo> getTokenConfigsByGroup(long orgId, List<String> tokens, String group) {
//        return getTokenConfigs(orgId, tokens, group, new ArrayList<>(), null);
//    }
//
//    public List<BaseTokenConfigInfo> getTokenConfigs(long orgId, List<String> tokens, String group, List<String> keys, String language) {
//        List<BaseTokenConfig> grpcConfigs = grpcBaseConfigService.getBhBaseTokenConfigs(GetBaseTokenConfigsRequest.newBuilder()
//                .setOrgId(orgId)
//                .setGroup(group)
//                .addAllKey(CollectionUtils.isNotEmpty(keys) ? keys : new ArrayList<>())
//                .setLanguage(Strings.nullToEmpty(language))
//                .addAllToken(CollectionUtils.isNotEmpty(tokens) ? tokens : new ArrayList<>())
//                .build()).getBaseConfigList();
//        if (CollectionUtils.isEmpty(grpcConfigs)) {
//            return new ArrayList<>();
//        }
//        List<BaseTokenConfigInfo> configs = grpcConfigs.stream()
//                .filter(c -> c.getIsOpen())
//                .map(c -> {
//                    BaseTokenConfigInfo baseConfigInfo = new BaseTokenConfigInfo();
//                    BeanCopyUtils.copyPropertiesIgnoreNull(c, baseConfigInfo);
//                    baseConfigInfo.setConfKey(c.getKey());
//                    baseConfigInfo.setConfValue(c.getValue());
//                    return baseConfigInfo;
//                })
//                .collect(Collectors.toList());
//        return configs;
//    }
//
//
//
//    public SwitchStatus getTokenConfigSwitchStatus(long orgId, String token, String group, String key) {
//        return getTokenConfigSwitchStatus(orgId, token, group, key, null);
//    }
//
//    public SwitchStatus getTokenConfigSwitchStatus(long orgId, String token, String group, String key, String language) {
//        BaseTokenConfigInfo configInfo = getOneTokenConfig(orgId, token, group, key);
//        if (configInfo == null) {
//            return SwitchStatus.builder().existed(false).open(false).build();
//        }
//        return SwitchStatus.builder().existed(true).open(configInfo.getConfValue().toLowerCase().equals("true")).build();
//    }


    private static final List<String> BH_INI_GROUP_LIST = Lists.newArrayList(
            BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP
    );


    private static final List<String> BH_SYMBOL_INI_GROUP_LIST = Lists.newArrayList(

    );



    private static final List<String> BH_TOKEN_INI_GROUP_LIST = Lists.newArrayList(

    );

    @Data
    @Builder(builderClassName = "Builder", toBuilder = true)
    public static class ConfigData {
        private String key;

        private String value;

        private String extraValue;
    }

    private static Map<String, BaseBizConfigService.ConfigData> brokerConfigMap = ImmutableMap.of();

    private static Map<String, BaseBizConfigService.ConfigData> brokerSymbolConfigMap = ImmutableMap.of();

    private static Map<String, BaseBizConfigService.ConfigData> brokerTokenConfigMap = ImmutableMap.of();

    @PostConstruct
    @Scheduled(cron = "0/10 * * * * ?")
    public void initConfig() {

        //initBrokerBaseConfig();

        //initBrokerBaseSymbolConfig();

        //initBrokerBaseTokenConfig();

    }



    private void initBrokerBaseConfig() {
        Map<String, BaseBizConfigService.ConfigData> tempConfigMap = Maps.newHashMap();
        GetBaseConfigsByGroupRequest request = GetBaseConfigsByGroupRequest.newBuilder()
                .addAllGroup(BH_INI_GROUP_LIST)
                .build();
        List<BaseConfig> baseConfigs = grpcBaseConfigService.getBhBaseConfigsByGroup(request).getBaseConfigList();
        for (BaseConfig c : baseConfigs) {
            String key = buildBaseConfigKey(c.getOrgId(), c.getGroup(), c.getKey(), c.getLanguage());
            tempConfigMap.put(key, BaseBizConfigService.ConfigData.builder().key(c.getKey()).value(c.getValue()).extraValue(c.getExtraValue()).build());
        }
        brokerConfigMap = ImmutableMap.copyOf(tempConfigMap);
    }


    private void initBrokerBaseSymbolConfig() {

        Map<String, BaseBizConfigService.ConfigData> tempConfigMap = Maps.newHashMap();
        GetBaseConfigsByGroupRequest request = GetBaseConfigsByGroupRequest.newBuilder()
                .addAllGroup(BH_SYMBOL_INI_GROUP_LIST)
                .build();
        List<BaseSymbolConfig> brokerBaseSymbolConfigs = grpcBaseConfigService.getBhBaseSymbolConfigsByGroup(request).getBaseConfigList();
        for (BaseSymbolConfig c : brokerBaseSymbolConfigs) {
            String key = buildBaseSymbolConfigKey(c.getOrgId(), c.getSymbol(), c.getGroup(), c.getKey(), c.getLanguage());
            tempConfigMap.put(key, BaseBizConfigService.ConfigData.builder().key(c.getKey()).value(c.getValue()).extraValue(c.getExtraValue()).build());
        }
        brokerSymbolConfigMap = ImmutableMap.copyOf(tempConfigMap);
    }


    private void initBrokerBaseTokenConfig() {
        Map<String, BaseBizConfigService.ConfigData> tempConfigMap = Maps.newHashMap();
        GetBaseConfigsByGroupRequest request = GetBaseConfigsByGroupRequest.newBuilder()
                .addAllGroup(BH_SYMBOL_INI_GROUP_LIST)
                .build();
        List<BaseTokenConfig> baseConfigs = grpcBaseConfigService.getBhBaseTokenConfigsByGroup(request).getBaseConfigList();
        for (BaseTokenConfig c : baseConfigs) {
            String key = buildBaseTokenConfigKey(c.getOrgId(), c.getToken(), c.getGroup(), c.getKey(), c.getLanguage());
            tempConfigMap.put(key, BaseBizConfigService.ConfigData.builder().key(c.getKey()).value(c.getValue()).extraValue(c.getExtraValue()).build());
        }
        brokerTokenConfigMap = ImmutableMap.copyOf(tempConfigMap);
    }


    private String buildBaseSymbolConfigKey(long orgId, String symbol, String group, String key, String language) {
        return buildBaseConfigKey(orgId, group, key, language) + "-" + symbol;
    }

    private String buildBaseTokenConfigKey(long orgId, String token, String group, String key, String language) {
        return buildBaseConfigKey(orgId, group, key, language) + "-" + token;
    }


    private String buildBaseConfigKey(long orgId, String group, String key, String language) {
        return orgId + "-" + group + "-" + key + "-" + Strings.nullToEmpty(language);
    }


//
//    public ConfigData getBrokerBaseConfig(long orgId, String group, String key, String language) {
//        String configKey = buildBaseConfigKey(orgId, group, key, language);
//        return brokerConfigMap.get(configKey);
//    }

}
