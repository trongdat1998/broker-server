package io.bhex.broker.server.grpc.server.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.reflect.TypeToken;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.broker.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.BrokerConfigConstants;
import io.bhex.broker.server.domain.IndexAppDownloadCustomer;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 11:50 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class BrokerHomepageConfigService {

    @Autowired
    private BrokerMapper brokerMapper;

    @Autowired
    private BrokerBasicConfigMapper brokerBasicConfigMapper;

    @Autowired
    private BrokerLocaleConfigMapper brokerLocaleConfigMapper;

    @Autowired
    private BrokerFeatureConfigMapper featureConfigMapper;

    @Autowired
    private BrokerDictConfigMapper brokerDictConfigMapper;

    @Autowired
    private BrokerIndexCustomerConfigMapper brokerIndexCustomerConfigMapper;

    @Resource
    private AppVersionInfoMapper appVersionInfoMapper;
    @Resource
    private AppDownloadLocaleMapper appDownloadLocaleMapper;
    /**
     * 获取全部券商的自定义配置信息
     *
     * @return
     */
    public QueryBrokerConfigResponse listHomepageConfig(Header header) {
        QueryBrokerConfigResponse.Builder builder = QueryBrokerConfigResponse.newBuilder();
        List<BrokerConfigDetail> brokerConfigDetails = new ArrayList<>();
        BrokerConfigDetail brokerConfigDetail = processData(header.getOrgId(), null, BrokerConfigConstants.PASS_STATUS);
        if (null != brokerConfigDetail) {
            brokerConfigDetails.add(brokerConfigDetail);
        }
        builder.addAllBrokerConfigDetails(brokerConfigDetails);
        return builder.build();
    }



    private BrokerConfigDetail processData(Long orgId, String locale, Integer status) {
        //基本信息配置
        BrokerBasicConfig brokerConfig = getBrokerConfig(orgId, BrokerConfigConstants.PASS_STATUS);
        if (null != brokerConfig) {
            BrokerConfigDetail.Builder configBulider = BrokerConfigDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(brokerConfig, configBulider);
            Map<String, String> extraContactMap = new HashMap<>();
            if (StringUtils.isNotEmpty(brokerConfig.getExtraContact())) {
                extraContactMap = JsonUtil.defaultGson().fromJson(brokerConfig.getExtraContact(), new TypeToken<Map<String, String>>() {
                }.getType());
            }
            configBulider.putAllExtraContactInfo(extraContactMap);
            configBulider.setOrgId(brokerConfig.getBrokerId());

            // 获取客服地址
            BrokerDictConfig csCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    orgId, BrokerDictConfig.KEY_CUSTOMER_SERVICE);
            if (csCfg != null && csCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setCustomerService(csCfg.getConfigValue());
            }
            // 获取期权客服地址
            BrokerDictConfig optionCsCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    orgId, BrokerDictConfig.KEY_OPTION_CUSTOMER_SERVICE);
            if (optionCsCfg != null && optionCsCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setOptionCustomerService(optionCsCfg.getConfigValue());
            }
            // 获取期货客服地址
            BrokerDictConfig futuresCsCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    orgId, BrokerDictConfig.KEY_FUTURES_CUSTOMER_SERVICE);
            if (futuresCsCfg != null && futuresCsCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setFuturesCustomerService(futuresCsCfg.getConfigValue());
            }

            // 获取期货答题开关
            BrokerDictConfig futuresAnswerCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    orgId, BrokerDictConfig.KEY_FUTURES_OPEN_WITH_ANSWER);
            if (futuresAnswerCfg != null && futuresAnswerCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setFuturesOpenWithAnswer(Boolean.parseBoolean(futuresAnswerCfg.getConfigValue()));
            }

            // 获取期权答题开关
            BrokerDictConfig optionAnswerCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    orgId, BrokerDictConfig.KEY_OPTION_OPEN_WITH_ANSWER);
            if (optionAnswerCfg != null && optionAnswerCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setOptionOpenWithAnswer(Boolean.parseBoolean(optionAnswerCfg.getConfigValue()));
            }

            List<BrokerLocaleConfig> brokerDetailConfigs = listDetailConfig(orgId, locale, BrokerConfigConstants.PASS_STATUS, true);
            List<BrokerFeatureConfig> brokerFeatureConfigs = listFeatureConfig(orgId, locale, BrokerConfigConstants.PASS_STATUS);

            //特点按语言分类
            Map<String, List<FeatureDetail>> featureConfigMap = new HashMap<>();
            for (BrokerFeatureConfig featureConfig : brokerFeatureConfigs) {
                String featureConfigLocale = featureConfig.getLocale();
                FeatureDetail.Builder b = FeatureDetail.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(featureConfig, b);
                b.setIndex(featureConfig.getRank());
                b.setOrgId(featureConfig.getBrokerId());
                if (featureConfigMap.containsKey(featureConfigLocale)) {
                    featureConfigMap.get(featureConfigLocale).add(b.build());
                } else {
                    List<FeatureDetail> list = new ArrayList<>();
                    list.add(b.build());
                    featureConfigMap.put(featureConfigLocale, list);
                }
            }

            //特点与对应语言的配置信息组合在一起
            List<LocaleDetail> localeDetails = brokerDetailConfigs.stream().map(d -> {
                LocaleDetail.Builder b = LocaleDetail.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(d, b);
                if (null != featureConfigMap.get(d.getLocale())) {
                    b.addAllFeatureDetails(featureConfigMap.get(d.getLocale()));
                }

                String footConfigJson = d.getFootConfig();
                if (StringUtils.isNotEmpty(footConfigJson)) {
                    JSONObject jo = JSON.parseObject(footConfigJson);
                    if (jo != null && jo.containsKey("enable") && jo.getInteger("enable") == 1) {
                        b.setFootConfigList(jo.getString("list"));
                    }
                }
                String headerConfigJson = d.getHeadConfig();
                if (StringUtils.isNotEmpty(headerConfigJson)) {
                    JSONObject jo = JSON.parseObject(headerConfigJson);
                    if (jo != null && jo.containsKey("enable") && jo.getInteger("enable") == 1) {
                        b.setHeadConfigList(jo.getString("list"));
                    }
                }

                b.setOrgId(d.getBrokerId());
                return b.build();
            }).collect(Collectors.toList());
            configBulider.addAllLocaleDetails(localeDetails);

            List<BrokerIndexCustomerConfig> indexCustomerConfigs = brokerIndexCustomerConfigMapper.getBrokerConfigs(orgId, status, BrokerIndexCustomerConfig.WEB_CONFIG);
            if (!CollectionUtils.isEmpty(indexCustomerConfigs)) {
                List<IndexCustomerModuleConfig> moduleConfigs = indexCustomerConfigs.stream()
                        .filter(c -> c.getOpenStatus() == 1)
                        .map(c -> {
                            IndexCustomerModuleConfig.Builder builder = IndexCustomerModuleConfig.newBuilder();
                            BeanCopyUtils.copyPropertiesIgnoreNull(c, builder);
                            if (StringUtils.isNotEmpty(c.getPlatform())) {
                                List<Integer> platform = Arrays.stream(c.getPlatform().split(",")).map(p -> Integer.parseInt(p)).collect(Collectors.toList());
                                builder.addAllPlatform(platform);
                            }
                            if (StringUtils.isNotEmpty(c.getUseModule())) {
                                List<Integer> useModule = Arrays.stream(c.getUseModule().split(",")).map(p -> Integer.parseInt(p)).collect(Collectors.toList());
                                builder.addAllUseModule(useModule);
                            }
                            if (c.getModuleName().equals("download")) {
                                builder.setContent(JsonUtil.defaultGson()
                                        .toJson(convertIndexAppDownloadCustomer(c.getBrokerId(), c.getLocale(), c.getContent())));
                            }
                            return builder.build();
                        }).collect(Collectors.toList());
                configBulider.addAllIndexCustomerModuleConfig(moduleConfigs);
            }
            return configBulider.build();
        }
        return null;
    }

    private IndexAppDownloadCustomer convertIndexAppDownloadCustomer(long brokerId, String locale, String content) {
        IndexAppDownloadCustomer downloadCustomer = JsonUtil.defaultGson().fromJson(content, IndexAppDownloadCustomer.class);
        List<Map<String, String>> iconList = downloadCustomer.getList();
        if (CollectionUtils.isEmpty(iconList)) {
            return downloadCustomer;
        }
        List<Map<String, String>> realIconList = new ArrayList<>();
        for (Map<String, String> iconMap : iconList) {
            if (!iconMap.containsKey("tag") || !iconMap.containsKey("link")) {
                continue;
            }
            if (StringUtils.isEmpty(iconMap.get("image"))) {
                continue;
            }
            if (iconMap.get("tag").equalsIgnoreCase("android") && StringUtils.isEmpty(iconMap.get("link"))) {
                AppVersionInfo appVersionInfo = appVersionInfoMapper.queryLastAppVersion(brokerId, "android");
                if (appVersionInfo != null) {
                    iconMap.put("link", appVersionInfo.getDownloadUrl());
                }
            } else if (iconMap.get("tag").equalsIgnoreCase("iphone") && StringUtils.isEmpty(iconMap.get("link"))) {
                AppDownloadLocale appDownloadLocale = appDownloadLocaleMapper.getAvailableRecord(brokerId, locale);
                if (appDownloadLocale != null) {
                    iconMap.put("link", appDownloadLocale.getDownloadWebUrl());
                }
            }
            realIconList.add(iconMap);
        }
        downloadCustomer.setList(realIconList);
        return downloadCustomer;
    }

    /**
     * 获取预览配置信息 只有首页定制模块有预览
     *
     * @param brokerId
     * @return
     */
    public BrokerConfigDetail previewConfig(Long brokerId, String locale) {
        Broker broker = brokerMapper.getByOrgId(brokerId);
        if (null != broker) {
            return processData(brokerId, locale, BrokerConfigConstants.PREVIEW_STATUS);
        }
        return null;
    }

    //*** admin ***

    /**
     * 一次性获取全部配置信息。三个部分：基础信息(不区分语言，通用配置)、多语言配置信息(区分语言的配置)、特点多语言配置信息(同一券商有多条的配置并且支持多语言)
     *
     * @param brokerId
     * @return
     */
    public AdminBrokerConfigDetail getBrokerWholeConfig(Long brokerId) {
        Broker broker = brokerMapper.getByOrgId(brokerId);
        if (null != broker) {
            AdminBrokerConfigDetail brokerConfigDetail = processWholeConfig(broker, null, BrokerConfigConstants.PASS_STATUS);
            return brokerConfigDetail;
        }
        return null;
    }

    private AdminBrokerConfigDetail processWholeConfig(Broker broker, String locale, Integer status) {
        if (null == broker) {
            return null;
        }
        //基本信息配置
        BrokerBasicConfig brokerConfig = getBrokerConfig(broker.getOrgId(), status);
        if (null != brokerConfig) {
            AdminBrokerConfigDetail.Builder configBulider = AdminBrokerConfigDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(brokerConfig, configBulider);
            if (StringUtils.isNotEmpty(broker.getFunctions())) {
                configBulider.setFunctionConfig(broker.getFunctions());
            }

            Map<String, String> extraContactMap = new HashMap<>();
            if (StringUtils.isNotEmpty(brokerConfig.getExtraContact())) {
                extraContactMap = JsonUtil.defaultGson().fromJson(brokerConfig.getExtraContact(), new TypeToken<Map<String, String>>() {
                }.getType());
            }
            configBulider.putAllExtraContactInfo(extraContactMap);

            // 获取客服地址
            BrokerDictConfig csCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    broker.getOrgId(), BrokerDictConfig.KEY_CUSTOMER_SERVICE);
            if (csCfg != null && csCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setCustomerService(csCfg.getConfigValue());
            }
            // 获取期权客服地址
            BrokerDictConfig optionCsCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    broker.getOrgId(), BrokerDictConfig.KEY_OPTION_CUSTOMER_SERVICE);
            if (optionCsCfg != null && optionCsCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setOptionCustomerService(optionCsCfg.getConfigValue());
            }
            // 获取期货客服地址
            BrokerDictConfig futuresCsCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    broker.getOrgId(), BrokerDictConfig.KEY_FUTURES_CUSTOMER_SERVICE);
            if (futuresCsCfg != null && futuresCsCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setFuturesCustomerService(futuresCsCfg.getConfigValue());
            }

            // 获取期货答题开关
            BrokerDictConfig futuresAnswerCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    broker.getOrgId(), BrokerDictConfig.KEY_FUTURES_OPEN_WITH_ANSWER);
            if (futuresAnswerCfg != null && futuresAnswerCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setFuturesOpenWithAnswer(futuresAnswerCfg.getConfigValue());
            }

            // 获取期权答题开关
            BrokerDictConfig optionAnswerCfg = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    broker.getOrgId(), BrokerDictConfig.KEY_OPTION_OPEN_WITH_ANSWER);
            if (optionAnswerCfg != null && optionAnswerCfg.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                configBulider.setOptionOpenWithAnswer(optionAnswerCfg.getConfigValue());
            }

            List<BrokerLocaleConfig> brokerDetailConfigs = listDetailConfig(broker.getOrgId(), locale, status, false);
            List<BrokerFeatureConfig> brokerFeatureConfigs = listFeatureConfig(broker.getOrgId(), locale, status);

            //特点按语言分类
            Map<String, List<AdminFeatureDetail>> featureConfigMap = new HashMap<>();
            for (BrokerFeatureConfig featureConfig : brokerFeatureConfigs) {
                String featureConfigLocale = featureConfig.getLocale();
                AdminFeatureDetail.Builder b = AdminFeatureDetail.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(featureConfig, b);
                b.setIndex(featureConfig.getRank());
                b.setBrokerId(featureConfig.getBrokerId());
                if (featureConfigMap.containsKey(featureConfigLocale)) {
                    featureConfigMap.get(featureConfigLocale).add(b.build());
                } else {
                    List<AdminFeatureDetail> list = new ArrayList<>();
                    list.add(b.build());
                    featureConfigMap.put(featureConfigLocale, list);
                }
            }

            //特点与对应语言的配置信息组合在一起
            List<AdminLocaleDetail> localeDetails = brokerDetailConfigs.stream().map(d -> {
                AdminLocaleDetail.Builder b = AdminLocaleDetail.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(d, b);
                if (null != featureConfigMap.get(d.getLocale())) {
                    b.addAllFeatureDetails(featureConfigMap.get(d.getLocale()));
                }
                return b.build();
            }).collect(Collectors.toList());
            configBulider.addAllLocaleDetails(localeDetails);
            return configBulider.build();
        }
        return null;
    }

    /**
     * 获取部分配置信息。基础信息(不区分语言，通用配置)
     *
     * @param brokerId
     * @return
     */
    public AdminBrokerConfigDetail getBrokerBasicConfig(Long brokerId) {
        BrokerBasicConfig brokerConfig = getBrokerConfig(brokerId, BrokerConfigConstants.PASS_STATUS);
        AdminBrokerConfigDetail.Builder builder = AdminBrokerConfigDetail.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(brokerConfig, builder);
        Map<String, String> extraContactMap = new HashMap<>();
        if (StringUtils.isNotEmpty(brokerConfig.getExtraContact())) {
            extraContactMap = JsonUtil.defaultGson().fromJson(brokerConfig.getExtraContact(), new TypeToken<Map<String, String>>() {
                                }.getType());
        }
        builder.putAllExtraContactInfo(extraContactMap);
        return builder.build();
    }

    /**
     * 获取部分配置信息。多语言配置信息(区分语言的配置)
     *
     * @param brokerId
     * @return
     */
    public GetLocaleDetailConfigReply getLocaleDetailConfigReply(Long brokerId) {
        List<BrokerLocaleConfig> detailConfigs = listDetailConfig(brokerId, null, BrokerConfigConstants.PASS_STATUS, false);
        GetLocaleDetailConfigReply.Builder builder = GetLocaleDetailConfigReply.newBuilder();
        List<AdminLocaleDetail> localeDetails = detailConfigs.stream().map(d -> {
            AdminLocaleDetail.Builder b = AdminLocaleDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(d, b);
            return b.build();
        }).collect(Collectors.toList());
        return builder.addAllLocaleDetails(localeDetails).build();
    }

    /**
     * 获取部分配置信息。特点多语言配置信息(同一券商有多条的配置并且支持多语言)
     *
     * @param brokerId
     * @return
     */
    public GetFeatureDetailConfigReply getFeatureDetailConfigReply(Long brokerId) {
        List<BrokerFeatureConfig> featureConfigs = listFeatureConfig(brokerId, null, BrokerConfigConstants.PASS_STATUS);
        GetFeatureDetailConfigReply.Builder builder = GetFeatureDetailConfigReply.newBuilder();
        List<AdminFeatureDetail> localeDetails = featureConfigs.stream().map(d -> {
            AdminFeatureDetail.Builder b = AdminFeatureDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(d, b);
            b.setIndex(d.getRank());
            return b.build();
        }).collect(Collectors.toList());
        return builder.addAllFeatureDetails(localeDetails).build();
    }

    /**
     * 一次性存储全部配置信息。三个部分：基础信息(不区分语言，通用配置)、多语言配置信息(区分语言的配置)、特点多语言配置信息(同一券商有多条的配置并且支持多语言)
     *
     * @param request
     * @return
     */
    @Transactional
    public SaveConfigReply saveBrokerWholeConfig(SaveBrokerConfigRequest request) {
        SaveConfigReply.Builder builder = SaveConfigReply.newBuilder();

        Broker broker = brokerMapper.getByOrgId(request.getBrokerId());
        if (null != broker) {
//            if (request.getSaveType() == SaveType.SAVE) {
//                broker.setApiDomain(request.getBrokerConfigDetail().getDomainHost());
//                brokerMapper.updateByPrimaryKey(broker);
//            }
            AdminBrokerConfigDetail brokerConfigDetail = request.getBrokerConfigDetail();
            if (null != brokerConfigDetail) {
                deleteOldConfig(request.getSaveType(), request.getBrokerId());
                Long brokerId = brokerConfigDetail.getBrokerId();
                List<AdminLocaleDetail> localeDetailsList = brokerConfigDetail.getLocaleDetailsList();

                saveBrokerBasicConfig(brokerConfigDetail, request.getSaveType(), brokerId);

                saveLocaleDetailConfig(localeDetailsList, request.getSaveType(), brokerId);

                saveBrokerDictConfig(brokerConfigDetail, request.getSaveType(), brokerId);

                //清空特点配置
                deleteFeatureConfig(request.getSaveType(), brokerId);
                for (AdminLocaleDetail localeDetail : localeDetailsList) {
                    saveFeatureDetailConfig(localeDetail.getFeatureDetailsList(), request.getSaveType(), brokerId);
                }
                builder.setResult(true);
            }
        } else {
            builder.setResult(false);
        }
        return builder.build();
    }

    /**
     * 清空之前的配置，如果为预览则清空之前为预览的配置
     * 如果为提交则清空当前展示的配置信息
     *
     * @param saveType
     * @param brokerId
     */
    private void deleteOldConfig(SaveType saveType, Long brokerId) {
        //清空基础配置
        deleteBasicConfig(saveType, brokerId);
        //清空多语言配置
        deleteLocaleConfig(saveType, brokerId);
        //清空特点配置
        deleteFeatureConfig(saveType, brokerId);
    }

    //清空基础配置
    private void deleteBasicConfig(SaveType saveType, Long brokerId) {
        Integer oldStatus = saveType == SaveType.SAVE ? BrokerConfigConstants.PASS_STATUS : BrokerConfigConstants.PREVIEW_STATUS;
        brokerBasicConfigMapper.deleteConfig(BrokerConfigConstants.DELETE_STATUS, oldStatus, brokerId);
    }

    //清空多语言配置
    private void deleteLocaleConfig(SaveType saveType, Long brokerId) {
        Integer oldStatus = saveType == SaveType.SAVE ? BrokerConfigConstants.PASS_STATUS : BrokerConfigConstants.PREVIEW_STATUS;
        brokerLocaleConfigMapper.deleteConfig(BrokerConfigConstants.DELETE_STATUS, oldStatus, brokerId);
    }

    //清空特点配置
    private void deleteFeatureConfig(SaveType saveType, Long brokerId) {
        Integer oldStatus = saveType == SaveType.SAVE ? BrokerConfigConstants.PASS_STATUS : BrokerConfigConstants.PREVIEW_STATUS;
        featureConfigMapper.deleteConfig(BrokerConfigConstants.DELETE_STATUS, oldStatus, brokerId);
    }

    /**
     * 存储部分配置信息。基础信息(不区分语言，通用配置)
     *
     * @param brokerConfigDetail
     * @param saveType
     * @return
     */
    public SaveConfigReply saveBrokerBasicConfig(AdminBrokerConfigDetail brokerConfigDetail, SaveType saveType, Long brokerId) {
        SaveConfigReply.Builder builder = SaveConfigReply.newBuilder();
        Integer status = saveType == SaveType.SAVE ? BrokerConfigConstants.PASS_STATUS : BrokerConfigConstants.PREVIEW_STATUS;
        //清空基础配置
        deleteBasicConfig(saveType, brokerId);
        BrokerBasicConfig brokerConfig = BrokerBasicConfig.builder()
                .brokerId(brokerId)
                .logo(brokerConfigDetail.getLogo())
                .domainHost(brokerConfigDetail.getDomainHost())
                .sslCrtFile(brokerConfigDetail.getSslCrtFile())
                .sslKeyFile(brokerConfigDetail.getSslKeyFile())
                .copyright(brokerConfigDetail.getCopyright())
                .favicon(brokerConfigDetail.getFavicon())
                .zendesk(brokerConfigDetail.getZendesk())
                .facebook(brokerConfigDetail.getFacebook())
                .twitter(brokerConfigDetail.getTwitter())
                .telegram(brokerConfigDetail.getTelegram())
                .reddit(brokerConfigDetail.getReddit())
                .wechat(brokerConfigDetail.getWechat())
                .weibo(brokerConfigDetail.getWeibo())
                .favicon(brokerConfigDetail.getFavicon())
                .extraContact(JsonUtil.defaultGson().toJson(brokerConfigDetail.getExtraContactInfoMap()))
                .status(status)
                .createdAt(System.currentTimeMillis())
                .logoUrl(brokerConfigDetail.getLogoUrl())
                .build();
        brokerBasicConfigMapper.insert(brokerConfig);

        builder.setResult(true);
        return builder.build();
    }

    /**
     * 存储部分配置信息。多语言配置信息(区分语言的配置)
     *
     * @param localeDetailsList
     * @param saveType
     * @return
     */
    public SaveConfigReply saveLocaleDetailConfig(List<AdminLocaleDetail> localeDetailsList, SaveType saveType, Long brokerId) {
        SaveConfigReply.Builder builder = SaveConfigReply.newBuilder();
        if (!CollectionUtils.isEmpty(localeDetailsList)) {
            Integer status = saveType == SaveType.SAVE ? BrokerConfigConstants.PASS_STATUS : BrokerConfigConstants.PREVIEW_STATUS;
            //清空多语言配置
            deleteLocaleConfig(saveType, brokerId);
            for (AdminLocaleDetail detail : localeDetailsList) {

                int enable = detail.getEnable();
                if (enable == 0) {
                    String footConfigJson = detail.getFootConfig();
                    if (StringUtils.isNotEmpty(footConfigJson)) {
                        JSONObject jo = JSON.parseObject(footConfigJson);
                        if (jo != null && jo.containsKey("enable") && jo.getInteger("enable") == 1) {
                            enable = 1;
                        }
                    }
                    String headerConfigJson = detail.getHeadConfig();
                    if (StringUtils.isNotEmpty(headerConfigJson)) {
                        JSONObject jo = JSON.parseObject(headerConfigJson);
                        if (jo != null && jo.containsKey("enable") && jo.getInteger("enable") == 1) {
                            enable = 1;
                        }
                    }
                }


                BrokerLocaleConfig detailConfig = BrokerLocaleConfig.builder()
                        .brokerId(brokerId)
                        .featureTitle(detail.getFeatureTitle())
                        .locale(detail.getLocale())
                        .browserTitle(detail.getBrowserTitle())
                        .enable(enable)
                        .footConfig(detail.getFootConfig())
                        .headConfig(detail.getHeadConfig())
                        .status(status)
                        .createdAt(System.currentTimeMillis())
                        .helpCenter("")
                        .legalDescription("")
                        .userAgreement(detail.getUserAgreement())
                        .privacyAgreement(detail.getPrivacyAgreement())
                        .seoDescription(detail.getSeoDescription())
                        .seoKeywords(detail.getSeoKeywords())
                        .build();
                brokerLocaleConfigMapper.insertSelective(detailConfig);
            }
            builder.setResult(true);
        } else {
            builder.setResult(false);
        }
        return builder.build();
    }

    /**
     * 存储部分配置信息。特点多语言配置信息(同一券商有多条的配置并且支持多语言)
     *
     * @param featureDetailsList
     * @param saveType
     * @return
     */
    public SaveConfigReply saveFeatureDetailConfig(List<AdminFeatureDetail> featureDetailsList, SaveType saveType, Long brokerId) {
        //如果单独调用此接口，需要单独清空之前的配置，否则会有脏数据 -> deleteFeatureConfig(saveType, brokerId);
        SaveConfigReply.Builder builder = SaveConfigReply.newBuilder();
        if (!CollectionUtils.isEmpty(featureDetailsList)) {
            Integer status = saveType == SaveType.SAVE ? BrokerConfigConstants.PASS_STATUS : BrokerConfigConstants.PREVIEW_STATUS;
            for (AdminFeatureDetail detail : featureDetailsList) {
                BrokerFeatureConfig featureConfig = BrokerFeatureConfig.builder()
                        .brokerId(brokerId)
                        .imageUrl(detail.getImageUrl())
                        .title(detail.getTitle())
                        .description(detail.getDescription())
                        .locale(detail.getLocale())
                        .rank(detail.getIndex())
                        .status(status)
                        .createdAt(System.currentTimeMillis())
                        .build();
                featureConfigMapper.insert(featureConfig);
            }
            builder.setResult(true);
        } else {
            builder.setResult(false);
        }

        return builder.build();
    }

    /**
     * 存储券商字典配置信息
     */
    public SaveConfigReply saveBrokerDictConfig(AdminBrokerConfigDetail brokerConfigDetail, SaveType saveType, Long brokerId) {
        SaveConfigReply.Builder builder = SaveConfigReply.newBuilder();

        Integer status = saveType == SaveType.SAVE ? BrokerConfigConstants.PASS_STATUS : BrokerConfigConstants.PREVIEW_STATUS;

        // 保存期货答题配置开关
        if (StringUtils.isNotEmpty(brokerConfigDetail.getFuturesOpenWithAnswer())) {
            saveBrokerDictConfig(brokerId, BrokerDictConfig.KEY_FUTURES_OPEN_WITH_ANSWER,
                    brokerConfigDetail.getFuturesOpenWithAnswer(), status);
        }

        // 保存期权答题配置开关
        if (StringUtils.isNotEmpty(brokerConfigDetail.getOptionOpenWithAnswer())) {
            saveBrokerDictConfig(brokerId, BrokerDictConfig.KEY_OPTION_OPEN_WITH_ANSWER,
                    brokerConfigDetail.getFuturesOpenWithAnswer(), status);
        }

        // 保存客服地址
        if (StringUtils.isNotEmpty(brokerConfigDetail.getCustomerService())) {
            saveBrokerDictConfig(brokerId, BrokerDictConfig.KEY_CUSTOMER_SERVICE,
                    brokerConfigDetail.getCustomerService(), status);
        }

        // 保存期权客服地址
        if (StringUtils.isNotEmpty(brokerConfigDetail.getOptionCustomerService())) {
            saveBrokerDictConfig(brokerId, BrokerDictConfig.KEY_OPTION_CUSTOMER_SERVICE,
                    brokerConfigDetail.getOptionCustomerService(), status);
        }

        // 保存期货客服地址
        if (StringUtils.isNotEmpty(brokerConfigDetail.getFuturesCustomerService())) {
            saveBrokerDictConfig(brokerId, BrokerDictConfig.KEY_FUTURES_CUSTOMER_SERVICE,
                    brokerConfigDetail.getFuturesCustomerService(), status);
        }

        return builder.setResult(true).build();
    }

    private void saveBrokerDictConfig(Long brokerId, String key, String value, Integer status) {
        BrokerDictConfig brokerDictConfig = brokerDictConfigMapper.getBrokerDictConfigByKey(
                brokerId, key);
        if (brokerDictConfig == null) {
            brokerDictConfig = BrokerDictConfig.builder()
                    .configKey(key)
                    .configValue(value)
                    .configType("default")
                    .createdAt(System.currentTimeMillis())
                    .status(status)
                    .build();
            brokerDictConfigMapper.insert(brokerDictConfig);
        } else {
            brokerDictConfig.setConfigValue(value);
            brokerDictConfig.setStatus(status);
            brokerDictConfigMapper.updateByPrimaryKeySelective(brokerDictConfig);
        }
    }

    private BrokerBasicConfig getBrokerConfig(Long brokerId, Integer status) {
        if (null != brokerId) {
            Example configExample = new Example(BrokerBasicConfig.class);
            Example.Criteria criteria = configExample.createCriteria();
            criteria.andEqualTo("brokerId", brokerId);
            criteria.andEqualTo("status", status);
            BrokerBasicConfig brokerDetailConfigs = brokerBasicConfigMapper.selectOneByExample(configExample);
            return brokerDetailConfigs;
        }
        return null;
    }

    private List<BrokerLocaleConfig> listDetailConfig(Long brokerId, String locale, Integer status, Boolean isEnable) {
        if (null != brokerId) {
            Example detailExample = new Example(BrokerLocaleConfig.class);
            Example.Criteria criteria = detailExample.createCriteria();
            criteria.andEqualTo("brokerId", brokerId);
            criteria.andEqualTo("status", status);
            if (isEnable) {
                criteria.andEqualTo("enable", BrokerLocaleConfig.ENABLE);
            }
            if (StringUtils.isNotEmpty(locale)) {
                criteria.andEqualTo("locale", locale);
            }
            List<BrokerLocaleConfig> brokerDetailConfigs = brokerLocaleConfigMapper.selectByExample(detailExample);
            return brokerDetailConfigs;
        }
        return new ArrayList<>();
    }

    private List<BrokerFeatureConfig> listFeatureConfig(Long brokerId, String locale, Integer status) {
        if (null != brokerId) {
            Example featureExample = new Example(BrokerFeatureConfig.class);
            Example.Criteria criteria = featureExample.createCriteria();
            criteria.andEqualTo("brokerId", brokerId);
            criteria.andEqualTo("status", status);
            if (StringUtils.isNotEmpty(locale)) {
                criteria.andEqualTo("locale", locale);
            }
            List<BrokerFeatureConfig> brokerFeatureConfigs = featureConfigMapper.selectByExample(featureExample);
            return brokerFeatureConfigs;
        }
        return new ArrayList<>();
    }


    @Transactional(rollbackFor = Exception.class)
    public SaveConfigReply editIndexCustomerConfig(List<IndexCustomerConfig> indexCustomerConfigs) {
        long brokerId = indexCustomerConfigs.get(0).getBrokerId();
        int status = indexCustomerConfigs.get(0).getStatus();
        List<String> modules = indexCustomerConfigs.stream().map(c -> c.getModuleName()).collect(Collectors.toList());
        modules.forEach(module -> {
            int rows = brokerIndexCustomerConfigMapper.deleteConfigs(brokerId, module, status);
            log.info("delete config org:{} module:{} status:{} rows : {}", brokerId, module, status, rows);
        });

        for (IndexCustomerConfig c : indexCustomerConfigs) {
            if (!c.getEnable()) {
                continue;
            }

            BrokerIndexCustomerConfig insertConfig = BrokerIndexCustomerConfig.builder()
                    .brokerId(c.getBrokerId())
                    .moduleName(c.getModuleName())
                    .locale(c.getLocale())
                    .openStatus(c.getOpen() ? 1 : 0)
                    .status(c.getStatus())
                    .content(c.getContent())
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .tabName(c.getTabName())
                    .type(c.getType())
                    .platform(c.getPlatform())
                    .useModule(c.getUseModule())
                    .configType(c.getConfigTypeValue())
                    .build();
            brokerIndexCustomerConfigMapper.insertSelective(insertConfig);

        }
        return SaveConfigReply.newBuilder().build();
    }

    public List<IndexCustomerConfig> getIndexCustomerConfig(long brokerId, int status, int configType) {
        List<BrokerIndexCustomerConfig> configs = brokerIndexCustomerConfigMapper.getBrokerConfigs(brokerId, status, configType);
        if (CollectionUtils.isEmpty(configs)) {
            return new ArrayList<>();
        }
        List<IndexCustomerConfig> grpcConfigs = configs.stream().map(c -> {
            IndexCustomerConfig.Builder builder = IndexCustomerConfig.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(c, builder);
            builder.setOpen(c.getOpenStatus() == 1);
            builder.setConfigTypeValue(c.getConfigType());
            return builder.build();
        }).collect(Collectors.toList());
        return grpcConfigs;
    }

    /**
     * {"featureTitle":"核心交易，赢在BHEX",
     *     "features":[
     *         {
     *             "title":"安全可信",
     *             "image":"https://static.nucleex.com/bhop/image/jvCfGhvPZkfWRz0Pt2B18M044LQn3_ggx0RwkfoFojQ.png",
     *             "description":"传统金融级别底层架构",
     *             "index":0
     *         }}
     * @param brokerId
     * @param switchToNewVersion
     * @return
     */

    public SaveConfigReply switchIndexCustomerConfig(long brokerId, boolean switchToNewVersion) {
        if (!switchToNewVersion) {
            return SaveConfigReply.newBuilder().build();
        }
        return SaveConfigReply.newBuilder().build();
    }

    public QueryAppIndexConfigsResponse queryAppIndexConfigs(QueryAppIndexConfigsRequest request) {
        QueryAppIndexConfigsResponse.Builder builder = QueryAppIndexConfigsResponse.newBuilder();
        List<BrokerIndexCustomerConfig> list = brokerIndexCustomerConfigMapper.getBrokerConfigs(request.getHeader().getOrgId(),
                BrokerConfigConstants.PASS_STATUS, BrokerIndexCustomerConfig.APP_CONFIG);
        if (CollectionUtils.isEmpty(list)) {
            return builder.build();
        }
        List<AppIndexCustomerModuleConfig> result = list.stream()
                .filter(c -> c.getOpenStatus() == 1)
                .map(c -> {
                    AppIndexCustomerModuleConfig.Builder configBuilder = AppIndexCustomerModuleConfig.newBuilder();
                    BeanCopyUtils.copyPropertiesIgnoreNull(c, configBuilder);
                    return configBuilder.build();
                }).collect(Collectors.toList());
        return builder.addAllIndexCustomerConfig(result).build();
    }


}
