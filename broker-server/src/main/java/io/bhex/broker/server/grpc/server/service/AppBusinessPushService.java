package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.common.SendBusinessPushRequest;
import io.bhex.broker.server.grpc.client.service.GrpcPushService;
import io.bhex.broker.server.model.AppPushDevice;
import io.bhex.broker.server.push.bo.AppPushDeviceIndex;
import io.bhex.broker.server.push.bo.AppPushDeviceSimple;
import io.bhex.broker.server.push.bo.OrgBusinessPushRecordSimple;
import io.bhex.broker.server.push.service.AppPushDeviceService;
import io.bhex.broker.server.push.service.OrgBusinessPushRecordService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 业务推送服务
 * @author wangshouchao
 */
@Slf4j
@Service
public class AppBusinessPushService {


    private final String UNDER_LINE = "_";

    @Resource
    private AppPushService appPushService;

    @Resource
    private AppPushDeviceService appPushDeviceService;

    @Resource
    private GrpcPushService grpcPushService;

    @Resource
    private OrgBusinessPushRecordService orgBusinessPushRecordService;

    private final ThreadPoolExecutor poolExecutor = getBusinessPushThreadPool();

    private ThreadPoolExecutor getBusinessPushThreadPool() {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 10, 2, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1000), new BasicThreadFactory.Builder().namingPattern("BusinessPushTask_%d").daemon(true).build(),
                (r, executor) -> log.error("business push task discard ..........!"));
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        return threadPoolExecutor;
    }

    /**
     * 对broker全部设备进行业务推送
     */
    public void sendBrokerAllDeviceBusinessPushAsync(OrgBusinessPushRecordSimple recordSimple, Long pushRecordId) throws Exception {
        if (pushRecordId <= 0) {
            throw new Exception("No push record!");
        }
        log.info("send broker all device business push!{},{},{}", recordSimple.getOrgId(), recordSimple.getBusinessType().name(), pushRecordId);
        poolExecutor.execute(() -> sendBrokerAllDeviceBusinessPush(recordSimple, pushRecordId));
    }

    /**
     * 对broker指定用户进行业务推送
     * 需要对userIds进行分批处理
     */
    public void sendBrokerBusinessPushAsync(OrgBusinessPushRecordSimple recordSimple, List<Long> userIds, Long pushRecordId) throws Exception {
        if (pushRecordId <= 0) {
            throw new Exception("No push record!");
        }
        log.info("send broker business push!{},{},{}", recordSimple.getOrgId(), recordSimple.getBusinessType().name(), pushRecordId);
        poolExecutor.execute(() -> sendBrokerBusinessPush(recordSimple, userIds, pushRecordId));
    }

    private void sendBrokerAllDeviceBusinessPush(OrgBusinessPushRecordSimple recordSimple, Long pushRecordId) {
        int sendCount = 0;
        try {
            Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> deviceIndexListMap = appPushService.getOrgAppPushDeviceMap().get(recordSimple.getOrgId());
            //获取预计发送的数量
            sendCount = deviceIndexListMap.values().stream().mapToInt(List::size).sum();
            handleDeviceIndexBusinessPush(recordSimple, pushRecordId, sendCount, deviceIndexListMap);
        } catch (Exception e) {
            log.error("sendBrokerAllDeviceBusinessPush error!{},{},{}", recordSimple, recordSimple.getBusinessType().name(), pushRecordId, e);
            orgBusinessPushRecordService.updateOrgBusinessPushRecord(pushRecordId, 2, e.getMessage(), sendCount);
        }
    }

    private void handleDeviceIndexBusinessPush(OrgBusinessPushRecordSimple recordSimple, Long pushRecordId, int sendCount, Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> deviceIndexListMap) {
        for (Map.Entry<AppPushDeviceIndex, List<AppPushDeviceSimple>> entry : deviceIndexListMap.entrySet()) {
            AppPushDeviceIndex index = entry.getKey();
            List<AppPushDeviceSimple> simpleList = entry.getValue();
            sendPushBusinessRequest(recordSimple, pushRecordId, index, simpleList);
        }
        //更新发送结果(只要触发就不管)
        orgBusinessPushRecordService.updateOrgBusinessPushRecord(pushRecordId, 1, "success", sendCount);
    }

    private void sendBrokerBusinessPush(OrgBusinessPushRecordSimple recordSimple, List<Long> userIds, Long pushRecordId) {
        int sendCount = 0;
        try {
            final Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> deviceIndexListMap = new HashMap<>(16);
            if (userIds.size() > 1000) {
                //大量userId
                int groupNum = userIds.size() / 1000;
                for (int i = 0; i < groupNum; i++) {
                    List<Long> userIdList = userIds.subList(i * 1000, (i + 1) * 1000);
                    updateDeviceIndexLIstMap(recordSimple.getOrgId(), deviceIndexListMap, userIdList);
                }
                List<Long> atomicList = userIds.subList(groupNum * 1000, userIds.size());
                if (atomicList.size() > 0) {
                    updateDeviceIndexLIstMap(recordSimple.getOrgId(), deviceIndexListMap, atomicList);
                }
            } else if (userIds.size() > 0) {
                updateDeviceIndexLIstMap(recordSimple.getOrgId(), deviceIndexListMap, userIds);
            }
            //获取预计发送的数量
            sendCount = deviceIndexListMap.values().stream().mapToInt(List::size).sum();
            handleDeviceIndexBusinessPush(recordSimple, pushRecordId, sendCount, deviceIndexListMap);
        } catch (Exception e) {
            log.error("sendBrokerBusinessPush error!{},{},{}", recordSimple.getOrgId(), recordSimple.getBusinessType().name(), pushRecordId, e);
            orgBusinessPushRecordService.updateOrgBusinessPushRecord(pushRecordId, 2, e.getMessage(), sendCount);
        }
    }

    private void updateDeviceIndexLIstMap(Long brokerId, Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> deviceIndexListMap, List<Long> userIdList) {
        List<AppPushDevice> deviceList = appPushDeviceService.getPushDeviceByUserIds(brokerId, userIdList);
        Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> groupDeviceMap = appPushService.handleAppPushDeviceGroup(deviceList);
        groupDeviceMap.forEach((key, value) -> {
            if (deviceIndexListMap.containsKey(key)) {
                deviceIndexListMap.get(key).addAll(value);
            } else {
                deviceIndexListMap.put(key, value);
            }
        });
    }

    private void sendPushBusinessRequest(OrgBusinessPushRecordSimple recordSimple, Long pushRecordId, AppPushDeviceIndex index, List<AppPushDeviceSimple> simpleList) {
        if (simpleList.size() > 100) {
            //大量设备token
            int groupNum = simpleList.size() / 100;
            for (int i = 0; i < groupNum; i++) {
                List<AppPushDeviceSimple> atomicList = simpleList.subList(i * 100, (i + 1) * 100);
                sendAtomicPushBusinessRequest(recordSimple, pushRecordId, index, atomicList);
            }
            List<AppPushDeviceSimple> atomicList = simpleList.subList(groupNum * 100, simpleList.size());
            if (atomicList.size() > 0) {
                sendAtomicPushBusinessRequest(recordSimple, pushRecordId, index, atomicList);
            }
        } else if (simpleList.size() > 0) {
            sendAtomicPushBusinessRequest(recordSimple, pushRecordId, index, simpleList);
        }
    }

    /**
     * 最小单位批量发送推送
     */
    private void sendAtomicPushBusinessRequest(OrgBusinessPushRecordSimple recordSimple, Long pushRecordId, AppPushDeviceIndex index, List<AppPushDeviceSimple> linkedList) {
        List<String> tokenList = linkedList.stream().map(AppPushDeviceSimple::getToken).collect(Collectors.toList());
        String reqOrderId = pushRecordId + UNDER_LINE + RandomStringUtils.randomAlphanumeric(8);
        SendBusinessPushRequest sendBusinessPushRequest = SendBusinessPushRequest.newBuilder()
                .addAllPushToken(tokenList)
                .setOrgId(recordSimple.getOrgId())
                .setAppId(index.getAppId())
                .setAppChannel(index.getAppChannel())
                .setPushChannel(index.getThirdPushType())
                .setBusinessType(recordSimple.getBusinessType().name())
                .setLanguage(index.getLanguage())
                .putAllReqParam(recordSimple.getReqParam())
                .putAllPushUrlData(recordSimple.getUrlParam())
                .setRecordId(pushRecordId)
                .setReqOrderId(reqOrderId)
                .build();
        Boolean result = grpcPushService.sendBusinessPush(sendBusinessPushRequest);
        if (Boolean.FALSE.equals(result)) {
            //收到明确的失败
            log.info("send business push fail!{},{},{},{},{},{}", recordSimple.getOrgId(), recordSimple.getBusinessType().name(), index.getDeviceType(), index.getLanguage(), reqOrderId, linkedList.get(0).getId());
        }
    }
}
