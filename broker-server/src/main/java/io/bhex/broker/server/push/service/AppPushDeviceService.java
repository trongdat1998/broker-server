package io.bhex.broker.server.push.service;

import com.google.common.base.Strings;
import io.bhex.broker.grpc.app_push.PushDevice;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.model.AppPushDevice;
import io.bhex.broker.server.primary.mapper.AppPushDeviceMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 用户推送设备表
 * <p>
 * author: wangshouchao
 * Date: 2020/07/25 06:26:07
 */
@Service
@Slf4j
public class AppPushDeviceService {

    @Resource
    private AppPushDeviceMapper appPushDeviceMapper;

    public int cancelUserTokenPush(String appId, String deviceToken) {
        return appPushDeviceMapper.cancelUserTokenPush(appId, deviceToken);
    }

    /**
     *  根据userIds查询设备(调用的地方精简缓存对象控制)
     *  建议userIDS（在1000以内）
     */
    public List<AppPushDevice> getPushDeviceByUserIds(Long orgId, List<Long> userIds) {
        return appPushDeviceMapper.queryByUserIds(orgId, userIds);
    }

    /**
     * 该接口是个大型查询接口（注意查询参数是严格简化）
     */
    public List<AppPushDevice> getPushDeviceByOrgId(Long orgId, Long startId, Integer limit) {
        return appPushDeviceMapper.queryByOrgId(orgId, startId, limit);
    }

    public AppPushDevice getPushDevice(Long userId) {
        try {
            if (userId <= 0) {
                return null;
            }
            AppPushDevice appPushDevice = AppPushDevice.builder().userId(userId).build();
            List<AppPushDevice> appPushDevices = appPushDeviceMapper.queryByUserId(appPushDevice);

            if (!appPushDevices.isEmpty()) {
                return appPushDevices.get(0);
            } else {
                log.warn("No pushDevice found");
                return null;
            }
        } catch (Exception e) {
            log.error("error get pushDevice with", e.fillInStackTrace());
        }

        return null;
    }

    @Async
    public void addPushDevice(Header header, PushDevice pushDevice) {
        if (Strings.isNullOrEmpty(pushDevice.getAppId()) || Strings.isNullOrEmpty(pushDevice.getAppVersion())
                || Strings.isNullOrEmpty(pushDevice.getDeviceType()) || Strings.isNullOrEmpty(pushDevice.getDeviceToken())
                || Strings.isNullOrEmpty(pushDevice.getThirdPushType()) || Strings.isNullOrEmpty(pushDevice.getClientId())) {
            return;
        }
        Long timestamp = System.currentTimeMillis();
        AppPushDevice appPushDevice = AppPushDevice.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .appId(pushDevice.getAppId())
                .appVersion(pushDevice.getAppVersion())
                .deviceType(pushDevice.getDeviceType())
                .deviceVersion(pushDevice.getDeviceVersion())
                .appChannel(pushDevice.getAppChannel())
                .deviceToken(pushDevice.getDeviceToken())
                .clientId(pushDevice.getClientId())
                .thirdPushType(pushDevice.getThirdPushType())
                .pushLimit(pushDevice.getPushLimit())
                .language(header.getLanguage() == null ? "zh_CN" : header.getLanguage())
                .created(timestamp)
                .updated(timestamp)
                .build();
        List<AppPushDevice> pushDeviceList;
        if (header.getUserId() > 0L) {
            //如果userId大于0，每种推送渠道只绑定最新的设备
            AppPushDevice queryObj = new AppPushDevice();
            queryObj.setDeviceType(pushDevice.getDeviceType());
            queryObj.setUserId(pushDevice.getUserId());
            queryObj.setThirdPushType(pushDevice.getThirdPushType());
            pushDeviceList = appPushDeviceMapper.queryByUserIdForAddDevice(queryObj);
            //查询当前设备token关联未登录的设备,查询没有userId的条件
            AppPushDevice queryByTokenObj = new AppPushDevice();
            //queryByTokenObj.setUserId(0L);
            queryByTokenObj.setDeviceType(pushDevice.getDeviceType());
            queryByTokenObj.setDeviceToken(pushDevice.getDeviceToken());
            queryByTokenObj.setThirdPushType(pushDevice.getThirdPushType());
            List<AppPushDevice> pushDeviceByTokenList = appPushDeviceMapper.queryByDeviceToken(queryByTokenObj);
            pushDeviceByTokenList = pushDeviceByTokenList.stream()
                    .filter(device -> device.getUserId() == 0L)
                    .collect(Collectors.toList());
            pushDeviceList.addAll(pushDeviceByTokenList);
        } else {
            //查询当前设备token关联登录和未登录的设备
            AppPushDevice queryByTokenObj = new AppPushDevice();
            queryByTokenObj.setDeviceType(pushDevice.getDeviceType());
            queryByTokenObj.setDeviceToken(pushDevice.getDeviceToken());
            queryByTokenObj.setThirdPushType(pushDevice.getThirdPushType());
            pushDeviceList = appPushDeviceMapper.queryByDeviceToken(queryByTokenObj);
        }
        long size = pushDeviceList.size();
        if (size == 0) {
            appPushDeviceMapper.insert(appPushDevice);
        } else {
            appPushDevice.setId(pushDeviceList.get(0).getId());
            //未登录用户不覆盖原有记录的userId
            appPushDevice.setUserId(appPushDevice.getUserId() == 0 ? pushDeviceList.get(0).getUserId() : appPushDevice.getUserId());
            appPushDevice.setCreated(pushDeviceList.get(0).getCreated());
            appPushDeviceMapper.update(appPushDevice);
            if (size > 1) {
                for (int index = 1; index < size; index++) {
                    appPushDeviceMapper.delete(pushDeviceList.get(index).getId());
                }
            }
        }
    }

}