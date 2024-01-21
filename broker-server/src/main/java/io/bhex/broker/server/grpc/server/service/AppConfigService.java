/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.grpc.app_config.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.AppChannel;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.model.AppDownloadLocale;
import io.bhex.broker.server.model.AppVersionConfigLog;
import io.bhex.broker.server.model.AppVersionInfo;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.AppVersionCompare;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AppConfigService {

    public static final int IS_TRUE = 1;

    @Resource
    private AppVersionConfigMapper appVersionConfigMapper;

    @Resource
    private AppVersionInfoMapper appVersionInfoMapper;

    @Resource
    private AppDownloadLocaleMapper appDownloadLocaleMapper;

    @Resource
    private AppVersionConfigLogMapper appVersionConfigLogMapper;

    @Resource
    private AppIndexModuleConfigMapper appIndexModuleConfigMapper;
    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Deprecated
    public CheckAppVersionResponse checkAppVersion(Header header, String appId, String appVersion, String appChannel,
                                                   String deviceType) {
        if (Strings.isNullOrEmpty(appId) || Strings.isNullOrEmpty(appVersion) || Strings.isNullOrEmpty(deviceType)) {
            return CheckAppVersionResponse.newBuilder()
                    .setVersionConfig(AppVersionConfig.newBuilder()
                            .setNeedUpdate(false)
                            .setNeedForceUpdate(false)
                            .build())
                    .build();
        }
        io.bhex.broker.server.model.AppVersionConfig queryObj = new io.bhex.broker.server.model.AppVersionConfig();
        queryObj.setOrgId(header.getOrgId());
        queryObj.setAppId(appId);
        queryObj.setAppVersion(appVersion);
        queryObj.setAppChannel(appChannel);
        queryObj.setDeviceType(deviceType);
        io.bhex.broker.server.model.AppVersionConfig versionConfig = appVersionConfigMapper.getByAppVersion(queryObj);
        if (versionConfig == null) {
            return CheckAppVersionResponse.newBuilder()
                    .setVersionConfig(AppVersionConfig.newBuilder()
                            .setNeedUpdate(false)
                            .setNeedForceUpdate(false)
                            .build())
                    .build();
        }
        return CheckAppVersionResponse.newBuilder()
                .setVersionConfig(AppVersionConfig.newBuilder()
                        .setNeedUpdate(versionConfig.getNeedUpdate() == IS_TRUE)
                        .setNeedForceUpdate(versionConfig.getNeedForceUpdate() == IS_TRUE)
                        .setDownloadUrl(versionConfig.getDownloadUrl())
                        .build())
                .build();
    }

    public QueryAllAppVersionConfigResponse queryAllAppVersionConfig(Header header) {
        List<io.bhex.broker.server.model.AppVersionConfig> localAppVersionConfigList;
        List<AppVersionInfo> appVersionInfoList;
        if (header.getOrgId() != 0) {
            localAppVersionConfigList = appVersionConfigMapper.queryOrgEnableConfigs(header.getOrgId());
            appVersionInfoList = appVersionInfoMapper.queryOrgAppVersionInfo(header.getOrgId());
        } else {
            localAppVersionConfigList = appVersionConfigMapper.queryAllEnableConfigs();
            appVersionInfoList = appVersionInfoMapper.queryAll();
        }

        Map<String, AppVersionInfo> appVersionInfoMap = appVersionInfoList.stream()
                .collect(Collectors.toMap(AppVersionInfo::getUniqueKey, Function.identity()));
        List<AppVersionConfig> appVersionConfigList = localAppVersionConfigList.stream()
                .map(config -> {
                    AppVersionInfo appVersionInfo = appVersionInfoMap.get(config.getUpdateVersionUniqueKey());
                    return AppVersionConfig.newBuilder()
                            .setOrgId(config.getOrgId())
                            .setAppId(Strings.nullToEmpty(config.getAppId()))
                            .setAppVersion(Strings.nullToEmpty(config.getAppVersion()))
                            .setDeviceType(Strings.nullToEmpty(config.getDeviceType()))
                            .setDeviceVersion(Strings.nullToEmpty(config.getDeviceVersion()))
                            .setAppChannel(Strings.nullToEmpty(config.getAppChannel()))
                            .setNeedUpdate(config.getNeedUpdate() == IS_TRUE) // 是否需要升级
                            .setNeedForceUpdate(config.getNeedForceUpdate() == IS_TRUE) // 是否需要强升
                            .setNewVersion(config.getUpdateVersion()) // 升级版本信息

                            .setDownloadUrl(getAppDownloadUrl(appVersionInfo, config.getAppChannel(), config.getOrgId())) // 版本更新安装包下载url
                            .setNewFeature(appVersionInfo == null ? "" : appVersionInfo.getNewFeatures()) // 新版本特性
                            .setIsLatestVersion(appVersionInfo != null && appVersionInfo.getIsLatestVersion() == IS_TRUE) // 当前版本是否为最新版本
                            .setDownloadWebviewUrl(appVersionInfo == null ? "" : appVersionInfo.getDownloadWebviewUrl()) // 版本更新web url

                            .build();
                }).collect(Collectors.toList());
        return QueryAllAppVersionConfigResponse.newBuilder().addAllVersionConfig(appVersionConfigList).build();
    }

    private String getAppDownloadUrl(AppVersionInfo appVersionInfo, String configChannel, long orgId) {
        AppChannel appChannel = AppChannel.getByValue(configChannel);
        if (appChannel == null) {
            return "";
        }

        String confKey = null;
        switch (appChannel) {
            case GOOGLEPLAY:
                confKey = BaseConfigConstants.GOOGLE_PLAY_URL;
                break;
            case APPSTORE:
                confKey = BaseConfigConstants.APP_STORE_URL;
                break;
            case TESTFLIGHT:
                confKey = BaseConfigConstants.TESTFIGHT_URL;
                break;
        }
        if (confKey != null) {
            BaseConfigInfo configInfo = baseBizConfigService.getOneConfig(orgId,
                    BaseConfigConstants.APP_DOWNLOAD_URL_GROUP, confKey);
            return configInfo != null ? configInfo.getConfValue() : "";
        }

        //官方通道
        return appVersionInfo != null ? appVersionInfo.getDownloadUrl() : "";
    }

    @Transactional
    public EditAppIndexIconResponse editAppIndexModule(EditAppIndexModuleRequest request) {
        appIndexModuleConfigMapper.resetModuleConfig(request.getOrgId(), request.getModuleType());
        List<io.bhex.broker.grpc.app_config.AppIndexModuleConfig> configs = request.getAppIndexModuleList();
        Map<String, List<io.bhex.broker.grpc.app_config.AppIndexModuleConfig>> groups = configs.stream()
                .collect(Collectors.groupingBy(io.bhex.broker.grpc.app_config.AppIndexModuleConfig::getLanguage));

        for (String language : groups.keySet()) {
            List<io.bhex.broker.grpc.app_config.AppIndexModuleConfig> list = groups.get(language);
            for (int i = 0; i < list.size(); i++) {
                io.bhex.broker.grpc.app_config.AppIndexModuleConfig c = list.get(i);
                io.bhex.broker.server.model.AppIndexModuleConfig moduleConfig = new io.bhex.broker.server.model.AppIndexModuleConfig();
                BeanCopyUtils.copyPropertiesIgnoreNull(list.get(i), moduleConfig);
                moduleConfig.setCreated(System.currentTimeMillis());
                moduleConfig.setOrgId(request.getOrgId());
                moduleConfig.setStatus(1);
                moduleConfig.setCustomOrder(list.size() - i);
                moduleConfig.setModuleType(request.getModuleType());
                moduleConfig.setJumpType(c.getJumpTypeValue());
                moduleConfig.setLoginShow(c.getLoginShowValue());
                moduleConfig.setNeedLogin(c.getNeedLogin() ? 1 : 0);
                appIndexModuleConfigMapper.insertSelective(moduleConfig);
            }
        }
        return EditAppIndexIconResponse.newBuilder().build();
    }

    public ListAppIndexModulesResponse listAppIndexModules(ListAppIndexModulesRequest request) {
        List<io.bhex.broker.server.model.AppIndexModuleConfig> dbConfigs = appIndexModuleConfigMapper.getModuleConfigs(request.getOrgId(), request.getModuleType());
        if (CollectionUtils.isEmpty(dbConfigs)) {
            return ListAppIndexModulesResponse.newBuilder().build();
        }
        List<io.bhex.broker.grpc.app_config.AppIndexModuleConfig> resultList = dbConfigs.stream().map(c -> {
            io.bhex.broker.grpc.app_config.AppIndexModuleConfig.Builder config = io.bhex.broker.grpc.app_config.AppIndexModuleConfig.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(c, config);
            config.setNeedLogin(c.getNeedLogin() == 1);
            config.setJumpTypeValue(c.getJumpType());
            config.setLoginShowValue(c.getLoginShow());
            return config.build();
        }).collect(Collectors.toList());
        return ListAppIndexModulesResponse.newBuilder().addAllAppIndexModule(resultList).build();
    }

    public QueryAllAppIndexFunctionConfigResponse queryAllAppIndexFunctionConfig(Header header) {
        List<io.bhex.broker.server.model.AppIndexModuleConfig> appIndexModuleConfigList;
        if (header.getOrgId() != 0) {
            appIndexModuleConfigList = appIndexModuleConfigMapper.getOrgAvailableConfigs(header.getOrgId());
        } else {
            appIndexModuleConfigList = appIndexModuleConfigMapper.getAllAvailableConfigs();
        }
        List<io.bhex.broker.grpc.app_config.AppIndexModuleConfig> responseData = appIndexModuleConfigList.stream()
                .filter(moduleConfig -> moduleConfig.getStatus() == 1)
                .map(moduleConfig -> io.bhex.broker.grpc.app_config.AppIndexModuleConfig.newBuilder()
                        .setOrgId(moduleConfig.getOrgId())
                        .setLanguage(moduleConfig.getLanguage())
                        .setModuleName(Strings.nullToEmpty(moduleConfig.getModuleName()))
                        .setModuleIcon(Strings.nullToEmpty(moduleConfig.getModuleIcon()))
                        .setJumpType(io.bhex.broker.grpc.app_config.AppIndexModuleConfig.JumpType.forNumber(moduleConfig.getJumpType()))
                        .setJumpUrl(moduleConfig.getJumpUrl())
                        .setNeedLogin(moduleConfig.getNeedLogin() == IS_TRUE)
                        .setCustomOrder(moduleConfig.getCustomOrder())
                        .setModuleType(moduleConfig.getModuleType())
                        .setLoginShow(io.bhex.broker.grpc.app_config.AppIndexModuleConfig.LoginShow.forNumber(moduleConfig.getLoginShow()))
                        .setLightDefaultIcon(moduleConfig.getLightDefaultIcon())
                        .setLightSelectedIcon(moduleConfig.getLightSelectedIcon())
                        .setDarkDefaultIcon(moduleConfig.getDarkDefaultIcon())
                        .setDarkSelectedIcon(moduleConfig.getDarkSelectedIcon())
                        .build())
                .collect(Collectors.toList());
        return QueryAllAppIndexFunctionConfigResponse.newBuilder().addAllIndexModuleConfig(responseData).build();
    }

    public void addNewApp(Long orgId, String appId, String appVersion, String deviceType, String deviceVersion, String appChannel,
                          String downloadUrl, String downloadWebviewUrl, String newFeatures, boolean updateToThisVersion) {
        Long currentTimestamp = System.currentTimeMillis();
        AppVersionInfo appVersionInfo = AppVersionInfo.builder()
                .orgId(orgId)
                .appId(appId)
                .appVersion(appVersion)
                .deviceType(deviceType)
                .deviceVersion(deviceVersion)
                .appChannel(appChannel)
                .downloadUrl(downloadUrl)
                .downloadWebviewUrl(downloadWebviewUrl)
                .newFeatures(newFeatures)
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        appVersionInfoMapper.insertSelective(appVersionInfo);
        if (updateToThisVersion) {
            appVersionConfigMapper.setAppUpdate(orgId, deviceVersion, appVersion);
        }
        io.bhex.broker.server.model.AppVersionConfig appVersionConfig =
                io.bhex.broker.server.model.AppVersionConfig.builder()
                        .orgId(orgId)
                        .appId(appId)
                        .appVersion(appVersion)
                        .deviceType(deviceType)
                        .deviceVersion(deviceVersion)
                        .appChannel(appChannel)
                        .needUpdate(0)
                        .needForceUpdate(0)
                        .updateVersion(appVersion)
                        .created(currentTimestamp)
                        .updated(currentTimestamp)
                        .build();
        appVersionConfigMapper.insertSelective(appVersionConfig);
    }

    @Transactional
    public SaveAppDownloadInfoResponse saveNewVersion(long orgId, AppDownloadInfoRequest request) {
        for (AppDownloadInfo info : request.getAppDownloadInfoList()) {
            AppVersionInfo existed = appVersionInfoMapper.queryOneAppVersion(orgId, info.getAppId(),
                    info.getAppVersion(), info.getDeviceType());

            Long currentTimestamp = System.currentTimeMillis();
            if (existed == null) {
                List<AppVersionInfo> oldVersions = appVersionInfoMapper.queryAppVersionByDeviceType(orgId, info.getAppId(), info.getDeviceType());
                boolean lower = oldVersions.stream()
                        .anyMatch(v -> AppVersionCompare.compare(info.getAppVersion(), v.getAppVersion()) < 0);
                if (lower) {
                    // 如果插入的版本号比已有的还小，就算了不能入库
                    return SaveAppDownloadInfoResponse.newBuilder().setRet(info.getDeviceType().equals("android") ? 1 : 2).build();
                }

                appVersionInfoMapper.updateLatestVerison(orgId, info.getDeviceType(), 0);

                AppVersionInfo appVersionInfo = AppVersionInfo.builder()
                        .orgId(orgId)
                        .appId(info.getAppId())
                        .appVersion(info.getAppVersion())
                        .deviceType(info.getDeviceType())
                        .deviceVersion("")
                        .appChannel(info.getAppChannel())
                        .downloadUrl(info.getDownloadUrl())
                        .downloadWebviewUrl("")
                        .newFeatures("")
                        .googlePlayDownloadUrl(info.getGooglePlayDownloadUrl())
                        .appStoreDownloadUrl(info.getAppStoreDownloadUrl())
                        .testflightDownloadUrl(info.getTestflightDownloadUrl())
                        .isLatestVersion(1)
                        .created(currentTimestamp)
                        .updated(currentTimestamp)
                        .build();
                appVersionInfoMapper.insertSelective(appVersionInfo);
                log.info("id:{}", appVersionInfo.getId());
            } else {
                existed.setAppStoreDownloadUrl(info.getAppStoreDownloadUrl());
                existed.setDownloadUrl(info.getDownloadUrl());
                existed.setAppStoreDownloadUrl(info.getAppStoreDownloadUrl());
                existed.setTestflightDownloadUrl(info.getTestflightDownloadUrl());
                existed.setDownloadWebviewUrl("");
                existed.setUpdated(currentTimestamp);
                existed.setGooglePlayDownloadUrl(info.getGooglePlayDownloadUrl());

                appVersionInfoMapper.updateByPrimaryKeySelective(existed);
            }
        }
        saveDownloadLocales(orgId, request.getAdminUserId(), request.getDownloadTypeValue(), request.getLocaleInfoList());
        return SaveAppDownloadInfoResponse.newBuilder().setRet(0).build();
    }

    private void saveDownloadLocales(long orgId, long adminUserId, int downloadType, List<AppDownloadLocaleInfo> localeInfos) {
        appDownloadLocaleMapper.deleteByBrokerId(orgId);
        for (AppDownloadLocaleInfo info : localeInfos) {
            AppDownloadLocale locale = new AppDownloadLocale();
            locale.setBrokerId(orgId);
            locale.setAndroidGuideImageUrl(info.getAndroidGuideImageUrl());
            locale.setIosGuideImageUrl(info.getIosGuideImageUrl());
            locale.setDownloadWebUrl(info.getDownloadWebUrl());
            locale.setLanguage(info.getLanguage());
            locale.setStatus(AppDownloadLocale.ON_STATUS);
            locale.setAdminUserId(adminUserId);
            locale.setDownloadType(downloadType);
            locale.setCreatedTime(System.currentTimeMillis());
            locale.setUpdatedTime(System.currentTimeMillis());
            appDownloadLocaleMapper.insertSelective(locale);
        }

    }

    public List<AppDownloadInfo> getAppDownloadInfos(long brokerId) {
        List<AppDownloadInfo> list = new ArrayList<>();
        AppVersionInfo androidAppInfo = appVersionInfoMapper.queryLastAppVersion(brokerId, "android");

        if (androidAppInfo != null) {
            AppDownloadInfo.Builder androidBuilder = AppDownloadInfo.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(androidAppInfo, androidBuilder);
            list.add(androidBuilder.build());
        }

        AppVersionInfo iosAppInfo = appVersionInfoMapper.queryLastAppVersion(brokerId, "ios");
        if (iosAppInfo != null) {
            AppDownloadInfo.Builder iosBuilder = AppDownloadInfo.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(iosAppInfo, iosBuilder);
            list.add(iosBuilder.build());
        }

        return list;
    }

    public List<io.bhex.broker.grpc.app_config.AppDownloadLocaleInfo> getAppDownloadLocales(long brokerId) {
        List<io.bhex.broker.grpc.app_config.AppDownloadLocaleInfo> list = new ArrayList<>();
        List<AppDownloadLocale> items = appDownloadLocaleMapper.getRecords(brokerId);
        if (CollectionUtils.isEmpty(items)) {
            return new ArrayList<>();
        }

        for (AppDownloadLocale local : items) {
            AppDownloadLocaleInfo.Builder builder = AppDownloadLocaleInfo.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(local, builder);
            list.add(builder.build());
        }
        return list;
    }

    public AppDownloadInfos queryAllDownInfo(Header header) {
        List<AppVersionInfo> allVersionInfo;
        List<AppDownloadLocale> locales;
        if (header.getOrgId() != 0) {
            allVersionInfo = appVersionInfoMapper.queryOrgLatest(header.getOrgId());
            locales = appDownloadLocaleMapper.getRecords(header.getOrgId());
        } else {
            allVersionInfo = appVersionInfoMapper.queryAllLatest();
            locales = appDownloadLocaleMapper.getAllAvailableRecords();
        }

        AppDownloadInfos.Builder builder = AppDownloadInfos.newBuilder();
        if (!CollectionUtils.isEmpty(allVersionInfo)) {
            List<AppDownloadInfo> downloadInfoList = allVersionInfo.stream().map(a -> {
                AppDownloadInfo.Builder builder1 = AppDownloadInfo.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(a, builder1);
                return builder1.build();
            }).collect(Collectors.toList());
            builder.addAllAppDownloadInfo(downloadInfoList);
        }

        if (!CollectionUtils.isEmpty(locales)) {
            List<AppDownloadLocaleInfo> localeInfoList = locales.stream().map(a -> {
                AppDownloadLocaleInfo.Builder builder1 = AppDownloadLocaleInfo.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(a, builder1);
                builder1.setOrgId(a.getBrokerId());
                return builder1.build();
            }).collect(Collectors.toList());
            builder.addAllLocaleInfo(localeInfoList);
        }
        return builder.build();
    }


    @Transactional
    public void saveAppUpdateInfo(SaveAppUpdateInfoRequest request) {
        String reqAppChannel = request.getAppUpdateInfo().getAppChannel();
        long orgId = request.getHeader().getOrgId();
        AppUpdateInfo appUpdateInfoReq = request.getAppUpdateInfo();
        Long currentTimestamp = System.currentTimeMillis();
        AppVersionInfo versionInfo = appVersionInfoMapper.queryLastAppVersion(orgId, appUpdateInfoReq.getDeviceType());

        JSONObject newFeatureJO = new JSONObject();
        List<AppUpdateInfo.NewFeature> localeInfos = appUpdateInfoReq.getNewFeatureList();
        for (AppUpdateInfo.NewFeature info : localeInfos) {
            newFeatureJO.put(info.getLanguage(), info.getDescription());
        }
        versionInfo.setNewFeatures(newFeatureJO.toJSONString());
        appVersionInfoMapper.updateByPrimaryKeySelective(versionInfo);

        String minVersion = appUpdateInfoReq.getMinVersion();
        String maxVersion = appUpdateInfoReq.getMaxVersion();

        io.bhex.broker.server.model.AppVersionConfig existedVersionConfig = appVersionConfigMapper
                .getByUpdateVersion(orgId, appUpdateInfoReq.getDeviceType(), reqAppChannel, versionInfo.getAppVersion());
        if (existedVersionConfig == null) {
            List<AppVersionInfo> fullVersions = appVersionInfoMapper.queryAllVersions(orgId, versionInfo.getDeviceType());
            List<String> updateVersions = appVersionConfigMapper.queryAllVersions(orgId, versionInfo.getDeviceType(), request.getAppUpdateInfo().getAppChannel());
            for (AppVersionInfo version : fullVersions) { //如果没有配置过升级的版本要重新加一次
                if (!updateVersions.contains(version.getAppVersion())) {
                    io.bhex.broker.server.model.AppVersionConfig appVersionConfig =
                            io.bhex.broker.server.model.AppVersionConfig.builder()
                                    .orgId(orgId)
                                    .appId(version.getAppId())
                                    .appVersion(version.getAppVersion())
                                    .deviceType(versionInfo.getDeviceType())
                                    .deviceVersion(versionInfo.getDeviceVersion())
                                    .appChannel(request.getAppUpdateInfo().getAppChannel())
                                    .needUpdate(0)
                                    .needForceUpdate(0)
                                    .enableStatus(1)
                                    .updateVersion(versionInfo.getAppVersion())
                                    .created(currentTimestamp)
                                    .updated(currentTimestamp)
                                    .build();
                    appVersionConfigMapper.insertSelective(appVersionConfig);
                }
            }
        }

        boolean forceUpdate = appUpdateInfoReq.getUpdateType() == AppUpdateInfo.UpdateType.FORCE_UPDATE;
        List<io.bhex.broker.server.model.AppVersionConfig> myVersionConfigs = appVersionConfigMapper
                .queryMyVersionConfigs(orgId, versionInfo.getDeviceType());
        for (io.bhex.broker.server.model.AppVersionConfig versionConfig : myVersionConfigs) {
            if (versionConfig.getAppVersion().equalsIgnoreCase(versionInfo.getAppVersion())) {
                continue; //最新版本不做处理
            }
            if (StringUtils.isEmpty(minVersion) || StringUtils.isEmpty(maxVersion)) { //全量升级
                versionConfig.setEnableStatus(1);
                versionConfig.setNeedUpdate(1);
                versionConfig.setNeedForceUpdate(forceUpdate ? 1 : 0);
                versionConfig.setUpdateVersion(versionInfo.getAppVersion());
                versionConfig.setUpdated(currentTimestamp);
                appVersionConfigMapper.updateByPrimaryKeySelective(versionConfig);
            } else if (AppVersionCompare.compare(minVersion, maxVersion) <= 0) {

                int v1 = AppVersionCompare.compare(versionConfig.getAppVersion(), minVersion);
                int v2 = AppVersionCompare.compare(versionConfig.getAppVersion(), maxVersion);

                if (v1 >= 0 && v2 <= 0) {
                    //当前版本在min和max之间
                    versionConfig.setEnableStatus(1);
                } else {
                    //versionConfig.setEnableStatus(0);
                    continue;
                }
                versionConfig.setUpdateVersion(versionInfo.getAppVersion());
                versionConfig.setNeedUpdate(1);
                versionConfig.setNeedForceUpdate(forceUpdate ? 1 : 0);
                versionConfig.setUpdated(currentTimestamp);
                appVersionConfigMapper.updateByPrimaryKeySelective(versionConfig);
            }
        }


        AppVersionConfigLog configLog = AppVersionConfigLog.builder()
                .orgId(orgId)
                .appChannel(request.getAppUpdateInfo().getAppChannel())
                .updateVersion(versionInfo.getAppVersion())
                .minVersion(minVersion)
                .maxVersion(maxVersion)
                .deviceType(versionInfo.getDeviceType())
                .updateType(appUpdateInfoReq.getUpdateTypeValue())
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .newFeatures(versionInfo.getNewFeatures())
                .groupId(request.getGroupId())
                .build();
        appVersionConfigLogMapper.insertSelective(configLog);

    }

    public List<AppUpdateInfo> queryAppUpdateLogs(long orgId) {
        List<String> deviceTypes = Lists.newArrayList("android", "ios");
        List<AppVersionConfigLog> logs = new ArrayList<>();
        for (String deviceType : deviceTypes) {
            List<String> channels = appVersionConfigMapper.getAppChannels(orgId, deviceType);
            if (CollectionUtils.isEmpty(channels)) {
                continue;
            }
            for (String appChannel : channels) {
                List<AppVersionConfigLog> theLogs = appVersionConfigLogMapper.queryLogsByChannel(orgId, deviceType, appChannel, 1);
                if (!CollectionUtils.isEmpty(theLogs)) {
                    logs.addAll(theLogs);
                }
            }
        }

        if (CollectionUtils.isEmpty(logs)) {
            return new ArrayList<>();
        }

        long groupId = System.currentTimeMillis();

        List<AppVersionConfigLog> noGroupLogs = logs.stream()
                .filter(s -> s.getGroupId() == 0)
                .collect(Collectors.toList());
        if (noGroupLogs.size() == logs.size()) {
            logs.forEach(log -> {
                log.setGroupId(groupId);
                appVersionConfigLogMapper.updateByPrimaryKeySelective(log);
            });
        }

        long maxGroupId = 0;
        for (AppVersionConfigLog log : logs) {
            maxGroupId = log.getGroupId() > maxGroupId ? log.getGroupId() : maxGroupId;
        }
        long theMaxGroupId = maxGroupId;
        logs = logs.stream()
                .filter(s -> s.getGroupId() == theMaxGroupId)
                .collect(Collectors.toList());
        List<AppUpdateInfo> list = new ArrayList<>();
        for (AppVersionConfigLog log : logs) {
            AppUpdateInfo.Builder builder = AppUpdateInfo.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(log, builder);
            builder.setLogId(log.getId());
            builder.setUpdateTypeValue(log.getUpdateType());

            AppUpdateInfo.NewFeature.Builder featureBuilder = AppUpdateInfo.NewFeature.newBuilder();
            Map<String, String> item = JSON.parseObject(log.getNewFeatures(), Map.class);
            for (String language : item.keySet()) {
                featureBuilder.setLanguage(language);
                featureBuilder.setDescription(item.get(language));
                builder.addNewFeature(featureBuilder.build());
            }
            list.add(builder.build());
        }
        return list;
    }

    public List<String> getAllVersions(long orgId, String deviceType) {
        List<AppVersionInfo> fullVersions = appVersionInfoMapper.queryAllVersions(orgId, deviceType);
        if (CollectionUtils.isEmpty(fullVersions) || fullVersions.size() <= 1) {
            return new ArrayList<>();
        }

        List<String> versions = fullVersions.stream()
                .map(v -> v.getAppVersion()).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(versions) || versions.size() <= 1) {
            return new ArrayList<>();
        }

        versions = versions.subList(0, versions.size() - 1); //不包含最大版本
        versions.sort((o1, o2) -> AppVersionCompare.compare(o1, o2));

        return versions;
    }

}
