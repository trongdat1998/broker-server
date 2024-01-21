package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.app_push.HuweiPushDeliveryCallbackRequest;
import io.bhex.broker.server.domain.HuaweiDeliveryReport;
import io.bhex.broker.server.model.AdminPushTask;
import io.bhex.broker.server.model.AdminPushTaskStatistics;
import io.bhex.broker.server.primary.mapper.AppBusinessPushRecordMapper;
import io.bhex.broker.server.push.bo.AdminPushDetailStatisticsSimple;
import io.bhex.broker.server.push.bo.AppPushStatisticsSimple;
import io.bhex.broker.server.push.bo.HuaweiDeliveryStatusEnum;
import io.bhex.broker.server.push.service.AdminPushTaskDetailService;
import io.bhex.broker.server.push.service.AdminPushTaskStatisticsService;
import io.bhex.broker.server.push.service.AppDeliveryReportService;
import io.bhex.broker.server.push.service.AppPushDeviceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author wangsc
 * @description 专门处理统计相关
 * @date 2020-07-30 14:59
 */
@Slf4j
@Service
public class AppPushStatisticsService {

    private final String UNDER_LINE = "_";

    @Resource
    private AppDeliveryReportService appDeliveryReportService;

    @Resource
    private AdminPushTaskStatisticsService adminPushTaskStatisticsService;

    @Resource
    private AppPushDeviceService appPushDeviceService;

    @Resource
    private AdminPushTaskDetailService adminPushTaskDetailService;

    @Resource
    private AppBusinessPushRecordMapper appBusinessPushRecordMapper;

    /**
     * key: taskId + _ + taskRound
     */
    private final ConcurrentHashMap<String, AppPushStatisticsSimple> statisticsCacheLoadingCache = new ConcurrentHashMap<>();

    /**
     * key:reqOrderId
     */
    private final ConcurrentHashMap<String, AdminPushDetailStatisticsSimple> taskDetailLoadingCache = new ConcurrentHashMap<>();

    private final ThreadPoolExecutor statisticsService = new ThreadPoolExecutor(20, 20,
            1, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(1000), new BasicThreadFactory.Builder().namingPattern("statisticsPush_%d").daemon(true).build(), (r, executor) ->
            log.info("push statistics discard ..........!"));

    @PostConstruct
    public void init() {
        statisticsService.allowCoreThreadTimeOut(true);
    }

    @Scheduled(initialDelay = 30_000, fixedRate = 30_000)
    public void handlePushTask() {
        //每分钟主动刺破没有被释放的缓存
        long currentTimeMillis = System.currentTimeMillis();
        taskDetailLoadingCache.forEach((key, value) -> {
            if (value.getExpireTime() < currentTimeMillis) {
                //并发的删除
                if (taskDetailLoadingCache.remove(key, value)) {
                    handleRemovePushDetailStatisticsSimple(value);
                }
            }
        });
        statisticsCacheLoadingCache.forEach((key, value) -> {
            if (value.getExpireTime() < currentTimeMillis) {
                //并发的删除
                if (statisticsCacheLoadingCache.remove(key, value)) {
                    handleRemoveStatisticsSimple(value);
                }
            }
        });
    }

    @PreDestroy
    public void invalidateAll() {
        //节点关闭时全部释放
        statisticsCacheLoadingCache.values().forEach(this::handleRemoveStatisticsSimple);
        taskDetailLoadingCache.values().forEach(this::handleRemovePushDetailStatisticsSimple);
    }

    private void handleRemoveStatisticsSimple(AppPushStatisticsSimple statisticsSimple) {
        if (statisticsSimple != null) {
            statisticsService.execute(() -> {
                try {
                    log.info("update push statistics!{}", JsonUtil.defaultGson().toJson(statisticsSimple));
                    int result = adminPushTaskStatisticsService.updateTaskStatisticsNum(statisticsSimple);
                    if (result <= 0) {
                        log.error("update push statistics fail!{},{}", statisticsSimple.getTaskId(), statisticsSimple.getTaskRound());
                    }
                } catch (Exception e) {
                    log.error("update push statistics error!{},{}", statisticsSimple.getTaskId(), statisticsSimple.getTaskRound(), e);
                }
            });
        }
    }

    private void handleRemovePushDetailStatisticsSimple(AdminPushDetailStatisticsSimple detailStatisticsSimple) {
        if (detailStatisticsSimple != null) {
            statisticsService.execute(() -> {
                try {
                    log.info("update push detail statistics!{}", JsonUtil.defaultGson().toJson(detailStatisticsSimple));
                    int result = adminPushTaskDetailService.updatePushDetailStatisticsNum(detailStatisticsSimple);
                    if (result <= 0) {
                        log.error("update push detail statistics fail!{}", detailStatisticsSimple.getReqOrderId());
                    }
                } catch (Exception e) {
                    log.error("update push detail statistics error!{}", detailStatisticsSimple.getReqOrderId(), e);
                }
            });
        }
    }

    /**
     * 用户点击回报
     * @param reqOrderId
     * @param deviceToken
     */
    public void pushClickCallback(String reqOrderId, String deviceToken) {
        //暂时不考虑重复点击(也无法保证多节点的重复点击)
        if (reqOrderId.startsWith("C")) {
            try {
                //只处理自定义的消息统计
                reqOrderId = reqOrderId.substring(1);
                if (reqOrderId.contains("_") && !reqOrderId.startsWith("test")) {
                    String[] params = reqOrderId.split("_");
                    long taskId = Long.parseLong(params[0]);
                    long taskRound = Long.parseLong(params[1]);
                    //更新主任务统计的点击数量,会有极限丢失一丢丢数据的情况
                    String cacheKey = taskId + UNDER_LINE + taskRound;
                    AppPushStatisticsSimple statisticsSimple = getAppPushStatisticsSimple(taskId, taskRound, cacheKey);
                    statisticsSimple.getClickCount().getAndIncrement();
                    //更新推送详情的点击数量,会有极限丢失一丢丢数据的情况
                    AdminPushDetailStatisticsSimple detailStatisticsSimple = getAdminPushDetailStatisticsSimple(reqOrderId);
                    detailStatisticsSimple.getClickCount().getAndIncrement();
                }
            } catch (Exception e) {
                log.error("handle push click callback error!{},{}", reqOrderId, deviceToken, e);
            }
        } else if (reqOrderId.startsWith("B")) {
            appBusinessPushRecordMapper.updateClickTime(reqOrderId.substring(1), System.currentTimeMillis());
        }
    }

    private AdminPushDetailStatisticsSimple getAdminPushDetailStatisticsSimple(String reqOrderId) {
        AdminPushDetailStatisticsSimple detailStatisticsSimple = taskDetailLoadingCache.get(reqOrderId);
        if (detailStatisticsSimple == null) {
            Long expireTime = System.currentTimeMillis() + 30_000L;
            detailStatisticsSimple = new AdminPushDetailStatisticsSimple(reqOrderId, expireTime);
            AdminPushDetailStatisticsSimple oldDetailStatisticsSimple = taskDetailLoadingCache.putIfAbsent(reqOrderId, detailStatisticsSimple);
            if (oldDetailStatisticsSimple != null) {
                detailStatisticsSimple = oldDetailStatisticsSimple;
            }
        }
        return detailStatisticsSimple;
    }

    /**
     * 华为设备送达回报
     * @param request
     */
    public void huaweiPushDeliveryCallback(HuweiPushDeliveryCallbackRequest request) {
        try {
            String body = request.getBody();
            HuaweiDeliveryReport originReport = JsonUtil.defaultGson().fromJson(body, HuaweiDeliveryReport.class);
            Map<String, List<HuaweiDeliveryReport.Status>> orderGroup = originReport.getStatuses().stream().collect(Collectors.groupingBy(HuaweiDeliveryReport.Status::getBiTag));
            for (String biTag : orderGroup.keySet()) {
                String reqOrderId = biTag.substring(1);
                long taskId = 0L;
                long taskRound = 1L;
                if (biTag.startsWith("C")) {
                    String[] params = reqOrderId.split("_");
                    taskId = Long.parseLong(params[0]);
                    taskRound = Long.parseLong(params[1]);
                    updateHuaweiPushDeliveryStatistics(reqOrderId, taskId, taskRound, orderGroup.get(biTag));
                } else if (biTag.startsWith("B")) {
                    updateHuaweiBizPushDeliveryStatus(reqOrderId, orderGroup.get(biTag));
                }
                appDeliveryReportService.insertHuaweiPushDelivery(request.getHeader().getOrgId(), reqOrderId, taskId, taskRound, JsonUtil.defaultGson().toJson(orderGroup.get(biTag)));
            }
        }catch (Exception e){
            log.error("handle huawei push delivery error!", e);
        }
    }

    /**
     * 在appPushService 锁内调用
     */
    protected void initPushAdminStatisticNum(AdminPushTask adminPushTask, long taskRound, long sendNum) {
        //判断是否存在,不存在插入初始化数据
        int count = adminPushTaskStatisticsService.queryCount(adminPushTask.getTaskId(), taskRound);
        if (count > 0) {
            log.warn("init push admin statistic repeat!{},{},{}", adminPushTask.getTaskId(), taskRound, count);
        } else {
            AdminPushTaskStatistics adminPushTaskStatistics = AdminPushTaskStatistics.builder()
                    .taskId(adminPushTask.getTaskId())
                    .taskRound(taskRound)
                    .orgId(adminPushTask.getOrgId())
                    .sendCount(sendNum)
                    .build();
            adminPushTaskStatisticsService.insert(adminPushTaskStatistics);
        }
    }

    private void updateHuaweiPushDeliveryStatistics(String reqOrderId, long taskId, long taskRound, List<HuaweiDeliveryReport.Status> statusList) {
        statisticsService.execute(() -> {
            Map<Integer, List<HuaweiDeliveryReport.Status>> statusGroup = statusList.stream().collect(Collectors.groupingBy(HuaweiDeliveryReport.Status::getStatus));
            statusGroup.forEach((key, value) -> {
                try {
                    String cacheKey = taskId + UNDER_LINE + taskRound;
                    //开发者需要对上述状态中的2、5、6、10、201做过滤处理，减少对这些用户的无效推送。
                    int size = value.size();
                    AppPushStatisticsSimple statisticsSimple = getAppPushStatisticsSimple(taskId, taskRound, cacheKey);
                    if (HuaweiDeliveryStatusEnum.SUCCESS.getStatus() == key) {
                        statisticsSimple.getDeliveryCount().getAndAdd(size);
                        statisticsSimple.getEffectiveCount().getAndAdd(size);
                        AdminPushDetailStatisticsSimple detailStatisticsSimple = getAdminPushDetailStatisticsSimple(reqOrderId);
                        detailStatisticsSimple.getDeliveryCount().getAndAdd(size);
                    } else if (HuaweiDeliveryStatusEnum.NO_APP.getStatus() == key || HuaweiDeliveryStatusEnum.NO_USER_TOKEN.getStatus() == key) {
                        statisticsSimple.getUninstallCount().getAndAdd(size);
                        cancelUserTokenPush(value);
                    } else if (HuaweiDeliveryStatusEnum.NO_DISPLAY.getStatus() == key) {
                        statisticsSimple.getUnsubscribeCount().getAndAdd(size);
                        cancelUserTokenPush(value);
                    } else if (HuaweiDeliveryStatusEnum.PUSH_CONTROL.getStatus() == key || HuaweiDeliveryStatusEnum.NO_ACTIVE_TOKEN.getStatus() == key) {
                        statisticsSimple.getCancelCount().getAndAdd(size);
                        cancelUserTokenPush(value);
                    }
                } catch (Exception e) {
                    log.error("load push statistics cache error!", e);
                }
            });
        });
    }

    private AppPushStatisticsSimple getAppPushStatisticsSimple(long taskId, long taskRound, String cacheKey) {
        AppPushStatisticsSimple statisticsSimple = statisticsCacheLoadingCache.get(cacheKey);
        if (statisticsSimple == null) {
            Long expireTime = System.currentTimeMillis() + 30_000L;
            statisticsSimple = new AppPushStatisticsSimple(taskId, taskRound, expireTime);
            AppPushStatisticsSimple oldStatisticsSimple = statisticsCacheLoadingCache.putIfAbsent(cacheKey, statisticsSimple);
            if (oldStatisticsSimple != null) {
                statisticsSimple = oldStatisticsSimple;
            }
        }
        return statisticsSimple;
    }

    private void updateHuaweiBizPushDeliveryStatus(String reqOrderId, List<HuaweiDeliveryReport.Status> statusList) {
        statisticsService.execute(() -> {
            statusList.forEach(s -> {
                appBusinessPushRecordMapper.updateDeliveryStatus(reqOrderId, s.getStatus().toString(), s.getTimestamp());
            });
        });
    }

    /**
     * 批量取消用户推送的权限
     */
    private void cancelUserTokenPush(List<HuaweiDeliveryReport.Status> list) {
        list.forEach(report -> {
            log.warn("cancel user token push!{},{},{}", report.getAppid(), report.getToken(), report.getStatus());
            appPushDeviceService.cancelUserTokenPush(report.getAppid(), report.getToken());
        });
    }
}
