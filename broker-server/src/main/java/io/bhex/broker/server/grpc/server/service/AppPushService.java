/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.bhex.base.common.MessageReply;
import io.bhex.base.common.SendPushRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.app_push.PushDevice;
import io.bhex.broker.grpc.app_push.SendAdminTestPushRequest;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.grpc.client.service.GrpcPushService;
import io.bhex.broker.server.model.AdminPushTask;
import io.bhex.broker.server.model.AdminPushTaskDetail;
import io.bhex.broker.server.model.AdminPushTaskLocale;
import io.bhex.broker.server.model.AppPushDevice;
import io.bhex.broker.server.push.bo.AppPushDeviceIndex;
import io.bhex.broker.server.push.bo.AppPushDeviceSimple;
import io.bhex.broker.server.push.bo.AppPushTaskCache;
import io.bhex.broker.server.push.bo.CycleTypeEnum;
import io.bhex.broker.server.push.bo.IHandleTaskStatus;
import io.bhex.broker.server.push.bo.PushTaskLocaleSlowCache;
import io.bhex.broker.server.push.bo.PushTaskStatusEnum;
import io.bhex.broker.server.push.bo.RedisStringLockUtil;
import io.bhex.broker.server.push.service.AdminPushTaskDetailService;
import io.bhex.broker.server.push.service.AdminPushTaskLocaleService;
import io.bhex.broker.server.push.service.AdminPushTaskService;
import io.bhex.broker.server.push.service.AppPushDeviceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AppPushService {

    private final String UNDER_LINE = "_";

    private final String AND = "&";

    private final String TASK_LOCK_PRE = "PUSH_TASK:";

    @Resource
    private GrpcPushService grpcPushService;

    @Resource
    private AppPushDeviceService appPushDeviceService;

    @Resource
    private AdminPushTaskService adminPushTaskService;

    @Resource
    private AdminPushTaskLocaleService adminPushTaskLocaleService;

    @Resource
    private AdminPushTaskDetailService adminPushTaskDetailService;

    @Resource(name = "pushTaskRedisTemplate")
    private RedisTemplate<Long, AppPushTaskCache> pushTaskRedisTemplate;

    @Resource
    private RedisStringLockUtil redisStringLockUtil;

    @Resource
    private AppPushStatisticsService appPushStatisticsService;

    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(20,
            new BasicThreadFactory.Builder().namingPattern("PushTask_%d").daemon(true).build());

    private final Map<Integer, IHandleTaskStatus> taskStatusHandleMap = new HashMap<>(8);

    /**
     * 缓存分组后的设备
     */
    private final LoadingCache<Long, Map<AppPushDeviceIndex, List<AppPushDeviceSimple>>> orgAppPushDeviceMap = CacheBuilder
            .newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build(new CacheLoader<Long, Map<AppPushDeviceIndex, List<AppPushDeviceSimple>>>() {
                @Override
                public Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> load(Long orgId) {
                    //循环获取
                    int limit = 10000;
                    Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> deviceIndexListMap = new HashMap<>(8);
                    long startId = 0;
                    while (limit == 10000) {
                        List<AppPushDevice> deviceList = appPushDeviceService.getPushDeviceByOrgId(orgId, startId, limit);
                        limit = deviceList.size();
                        if (limit > 0) {
                            startId = deviceList.get(limit - 1).getId();
                            Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> groupDeviceMap = handleAppPushDeviceGroup(deviceList);
                            groupDeviceMap.forEach((key, value) -> {
                                if (deviceIndexListMap.containsKey(key)) {
                                    deviceIndexListMap.get(key).addAll(value);
                                } else {
                                    deviceIndexListMap.put(key, value);
                                }
                            });
                        }
                    }
                    return deviceIndexListMap;
                }
            });

    /**
     * 缓存当前队列中的任务(缓存1小时)
     * 正常任务在30分钟内执行
     * key:taskId + "_" + taskRound
     */
    private final Cache<String, Boolean> taskExistCache = CacheBuilder
            .newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();

    /**
     * 缓存语言包
     * key: taskId + "&" + language
     */
    private final LoadingCache<String, PushTaskLocaleSlowCache> taskLanguage = CacheBuilder
            .newBuilder()
            .maximumSize(2000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build(new CacheLoader<String, PushTaskLocaleSlowCache>() {
                @Override
                public PushTaskLocaleSlowCache load(String key) {
                    String[] params = key.split(AND);
                    Long taskId = Long.valueOf(params[0]);
                    AdminPushTaskLocale adminPushTaskLocale = adminPushTaskLocaleService.getPushTaskLocale(taskId, params[1]);
                    return new PushTaskLocaleSlowCache(adminPushTaskLocale);
                }
            });

    @Scheduled(initialDelay = 0, fixedRate = 2 * 60_000)
    public void handlePushTask() {
        try {
            //定时处理推送任务(每次获取30分内未结束的任务或长时间未开始的任务),基于redis查询所有近期任务的执行情况
            List<AdminPushTask> list = adminPushTaskService.queryRecentlyPushTasks();
            //基于任务并发执行
            list.parallelStream().forEach(this::preHandleAdminPushTask);
        } catch (Exception e) {
            log.warn("handle push task error!", e);
        }
    }

    @PostConstruct
    public void init() {
        //初始化未结束的状态处理类
        taskStatusHandleMap.put(PushTaskStatusEnum.INITIAL.getStatus(), new PreStartTaskStatusHandle());
        taskStatusHandleMap.put(PushTaskStatusEnum.PUSHING.getStatus(), new PushingStatusTaskHandle());
        taskStatusHandleMap.put(PushTaskStatusEnum.PERIOD_FINISH.getStatus(), new PreStartTaskStatusHandle());
    }

    public LoadingCache<Long, Map<AppPushDeviceIndex, List<AppPushDeviceSimple>>> getOrgAppPushDeviceMap() {
        return orgAppPushDeviceMap;
    }

    public AppPushDevice getPushDevice(Long userId) {
        return appPushDeviceService.getPushDevice(userId);
    }

    @Async
    public void addPushDevice(Header header, PushDevice pushDevice) {
        appPushDeviceService.addPushDevice(header, pushDevice);
    }

    /**
     * 任务预处理(如果符合条件,进入延时执行队列)
     */
    private void preHandleAdminPushTask(AdminPushTask adminPushTask) {
        //判断是否已超过失效时间，超过调整当前状态为取消
        Long taskRound = getAdminPushTaskRound(adminPushTask);
        Long expireTime = adminPushTask.getExpireTime();
        if (expireTime != null && expireTime > System.currentTimeMillis()) {
            handleCancelAdminPushTask(adminPushTask, taskRound);
            return;
        }
        //判断当前当前任务当前轮次是否已在本地队列中,允许各节点各自存在一个延时任务节点间竞争执行
        Boolean existLocalTask = taskExistCache.getIfPresent(adminPushTask.getTaskId() + "_" + taskRound);
        if (existLocalTask == null || Boolean.FALSE.equals(existLocalTask)) {
            taskExistCache.put(adminPushTask.getTaskId() + "_" + taskRound, true);
            //判断进入延时任务
            long delaySec = (adminPushTask.getActionTime() - System.currentTimeMillis()) / 1000;
            if (delaySec <= 0) {
                log.warn("handle legacy admin push task!{},{}", adminPushTask.getTaskId(), adminPushTask.getActionTime());
                scheduler.execute(() -> handleAtomicAdminPushTask(adminPushTask, taskRound));
            } else {
                //延时准确至秒
                log.warn("plan handle admin push task!taskId={},delaySec={}", adminPushTask.getTaskId(), delaySec);
                scheduler.schedule(() -> handleAtomicAdminPushTask(adminPushTask, taskRound), delaySec, TimeUnit.SECONDS);
            }
        }
    }

    private void sendPushRequest(AdminPushTask adminPushTask, Long taskRound, AppPushDeviceIndex index, List<AppPushDeviceSimple> simpleList) throws ExecutionException {
        //判断语言为空字符的情况, 如果同时默认语言也为空就不发送
        if (StringUtils.isBlank(index.getLanguage()) && StringUtils.isBlank(adminPushTask.getDefaultLanguage())) {
            return;
        }
        AdminPushTaskLocale taskLocaleCache;
        if (StringUtils.isBlank(index.getLanguage())) {
            //直接取默认值的语言包
            taskLocaleCache = taskLanguage.get(adminPushTask.getTaskId() + AND + adminPushTask.getDefaultLanguage()).getAdminPushTaskLocale();
        } else {
            String cacheKey = adminPushTask.getTaskId() + AND + index.getLanguage();
            taskLocaleCache = taskLanguage.get(cacheKey).getAdminPushTaskLocale();
            if (taskLocaleCache == null && StringUtils.isNoneBlank(adminPushTask.getDefaultLanguage())) {
                //取默认值的语言包
                taskLocaleCache = taskLanguage.get(adminPushTask.getTaskId() + AND + adminPushTask.getDefaultLanguage()).getAdminPushTaskLocale();
            }
        }
        if (simpleList.size() > 100) {
            //大量设备token
            int groupNum = simpleList.size() / 100;
            for (int i = 0; i < groupNum; i++) {
                List<AppPushDeviceSimple> atomicList = simpleList.subList(i * 100, (i + 1) * 100);
                sendAtomicPushRequest(adminPushTask, taskLocaleCache, taskRound, index, atomicList);
            }
            List<AppPushDeviceSimple> atomicList = simpleList.subList(groupNum * 100, simpleList.size());
            if (atomicList.size() > 0) {
                sendAtomicPushRequest(adminPushTask, taskLocaleCache, taskRound, index, atomicList);
            }
        } else if (simpleList.size() > 0) {
            sendAtomicPushRequest(adminPushTask, taskLocaleCache, taskRound, index, simpleList);
        }
    }

    /**
     * 在分布式锁下更新
     */
    private void updateRedisPushTaskCache(AdminPushTask adminPushTask, Long taskRound, AppPushTaskCache appPushTaskCache, PushTaskStatusEnum pushTaskStatusEnum) {
        if (appPushTaskCache == null) {
            appPushTaskCache = new AppPushTaskCache();
            appPushTaskCache.setTaskId(adminPushTask.getTaskId());
            appPushTaskCache.setRetryNum(0);
            appPushTaskCache.setOrgId(adminPushTask.getOrgId());
        }
        appPushTaskCache.setTaskRound(taskRound);
        appPushTaskCache.setStatus(pushTaskStatusEnum.getStatus());
        appPushTaskCache.setUpdated(System.currentTimeMillis());
        pushTaskRedisTemplate.opsForValue().set(adminPushTask.getTaskId(), appPushTaskCache, 1, TimeUnit.DAYS);
    }

    /**
     * 更新失败重试次数
     */
    private void updateRedisPushTaskRetry(AdminPushTask adminPushTask, Long taskRound, AppPushTaskCache appPushTaskCache) {
        if (appPushTaskCache == null) {
            appPushTaskCache = new AppPushTaskCache();
            appPushTaskCache.setTaskRound(taskRound);
            appPushTaskCache.setTaskId(adminPushTask.getTaskId());
            appPushTaskCache.setRetryNum(1);
            appPushTaskCache.setOrgId(adminPushTask.getOrgId());
            appPushTaskCache.setStatus(PushTaskStatusEnum.PUSHING.getStatus());
        } else {
            appPushTaskCache.setRetryNum(appPushTaskCache.getRetryNum() + 1);
            log.error("handle push task retry!{},{},{}", adminPushTask.getTaskId(), taskRound, appPushTaskCache.getRetryNum());
            if (appPushTaskCache.getRetryNum() > 10 && adminPushTask.getCycleType() <= 2) {
                log.error("handle push task retry fail cancel!{},{},{}", adminPushTask.getTaskId(), taskRound, appPushTaskCache.getRetryNum());
                int finishResult = adminPushTaskService.updateAdminPushTaskCancelStatus(adminPushTask, "retry fail");
                if (finishResult > 0) {
                    updateRedisPushTaskCache(adminPushTask, taskRound, appPushTaskCache, PushTaskStatusEnum.CANCEL);
                }
            } else if (appPushTaskCache.getRetryNum() > 10) {
                log.error("handle push task retry fail cancel!{},{},{}", adminPushTask.getTaskId(), taskRound, appPushTaskCache.getRetryNum());
                int periodFinishResult = adminPushTaskService.updateAdminPushTaskPeriodFinishStatus(adminPushTask, "retry fail");
                if (periodFinishResult > 0) {
                    appPushTaskCache.setRetryNum(0);
                    updateRedisPushTaskCache(adminPushTask, taskRound, appPushTaskCache, PushTaskStatusEnum.PERIOD_FINISH);
                }
            }
        }
        appPushTaskCache.setUpdated(System.currentTimeMillis());
        pushTaskRedisTemplate.opsForValue().set(adminPushTask.getTaskId(), appPushTaskCache, 1, TimeUnit.DAYS);
    }

    /**
     * 最小单位批量发送推送
     */
    private void sendAtomicPushRequest(AdminPushTask adminPushTask, AdminPushTaskLocale adminPushTaskLocale, Long taskRound, AppPushDeviceIndex index, List<AppPushDeviceSimple> linkedList) {
        List<String> tokenList = linkedList.stream().map(AppPushDeviceSimple::getToken).collect(Collectors.toList());
        AdminPushTaskDetail adminPushTaskDetail = new AdminPushTaskDetail();
        String reqOrderId = adminPushTask.getTaskId() + UNDER_LINE + taskRound + UNDER_LINE + RandomStringUtils.randomAlphanumeric(8);
        if (adminPushTaskLocale == null) {
            log.info("push request fail no language! taskId={},thirdPushType={},language={}", adminPushTask.getTaskId(), index.getThirdPushType(), index.getLanguage());
            adminPushTaskDetail.setRemark("no language " + index.getLanguage());
        } else {
            SendPushRequest sendPushRequest = SendPushRequest.newBuilder()
                    .addAllPushToken(tokenList)
                    .setOrgId(adminPushTask.getOrgId())
                    .setAppId(index.getAppId())
                    .setAppChannel(index.getAppChannel())
                    .setPushChannel(index.getThirdPushType())
                    .setDeviceType(SendPushRequest.DeviceType.valueOf(index.getDeviceType().toUpperCase()))
                    .setReqOrderId(reqOrderId)
                    .setTitle(adminPushTaskLocale.getPushTitle())
                    .setContent(adminPushTaskLocale.getPushContent())
                    .setSummary(adminPushTaskLocale.getPushSummary())
                    .setUrl(adminPushTaskLocale.getPushUrl())
                    .setUrlType(adminPushTaskLocale.getUrlType())
                    .build();
            log.info("push request! reqOrderId:{},language:{},startId:{},endId{},size:{}", reqOrderId, index.getLanguage(), linkedList.get(0).getId(), linkedList.get(linkedList.size() - 1).getId(), linkedList.size());
            MessageReply messageReply = grpcPushService.sendPush(sendPushRequest);
            if (messageReply.getSuccess()) {
                adminPushTaskDetail.setRemark("success");
            } else {
                //收到明确的失败,就算发送成功
                log.error("send push fail!{},{},{},{},{}", reqOrderId, index.getThirdPushType(), index.getDeviceType(), index.getLanguage(), messageReply.getMessage());
                adminPushTaskDetail.setRemark(messageReply.getMessage());
            }
        }
        //存储实际请求记录
        long currentTimeMillis = System.currentTimeMillis();
        adminPushTaskDetail.setOrgId(adminPushTask.getOrgId());
        adminPushTaskDetail.setTaskId(adminPushTask.getTaskId());
        adminPushTaskDetail.setTaskRound(taskRound);
        adminPushTaskDetail.setReqOrderId(reqOrderId);
        adminPushTaskDetail.setAppId(index.getAppId());
        adminPushTaskDetail.setAppChannel(index.getAppChannel());
        adminPushTaskDetail.setThirdPushType(index.getThirdPushType());
        adminPushTaskDetail.setDeviceType(index.getDeviceType());
        adminPushTaskDetail.setLanguage(index.getLanguage());
        adminPushTaskDetail.setSendCount(linkedList.size());
        adminPushTaskDetail.setStartId(linkedList.get(0).getId());
        adminPushTaskDetail.setEndId(linkedList.get(linkedList.size() - 1).getId());
        adminPushTaskDetail.setActionTime(currentTimeMillis);
        adminPushTaskDetail.setCreated(currentTimeMillis);
        adminPushTaskDetail.setUpdated(currentTimeMillis);
        adminPushTaskDetailService.insert(adminPushTaskDetail);
    }

    private void handleCancelAdminPushTask(AdminPushTask adminPushTask, Long taskRound) {
        //失效取消当前任务,同时更新redis缓存状态
        String randomStr = RandomStringUtils.randomAlphanumeric(8);
        String lockKey = TASK_LOCK_PRE + adminPushTask.getTaskId();
        if (redisStringLockUtil.lock(lockKey, 60, 2, 500L, randomStr)) {
            try {
                AppPushTaskCache appPushTaskCache = pushTaskRedisTemplate.opsForValue().get(adminPushTask.getTaskId());
                if (appPushTaskCache == null || appPushTaskCache.getStatus() < 3) {
                    //不存在或者不是结束状态，更新数据库为取消状态
                    int result = adminPushTaskService.updateAdminPushTaskCancelStatus(adminPushTask, "expire cancel");
                    if (result > 0) {
                        updateRedisPushTaskCache(adminPushTask, taskRound, appPushTaskCache, PushTaskStatusEnum.CANCEL);
                    }
                }
            } catch (Exception e) {
                log.error("update admin push task cancel status error!{}", adminPushTask.getTaskId(), e);
            } finally {
                redisStringLockUtil.releaseLock(lockKey, randomStr);
            }
        } else {
            log.info("give up update admin push task cancel!");
        }
    }

    /**
     * 开始处理推送任务
     */
    private void handleAtomicAdminPushTask(AdminPushTask adminPushTask, Long taskRound) {
        //判断当前的执行状态
        IHandleTaskStatus handleTaskStatus = taskStatusHandleMap.get(adminPushTask.getStatus());
        if (handleTaskStatus == null) {
            log.error("unknown push task status!{},{}", adminPushTask.getTaskId(), adminPushTask.getStatus());
        } else {
            handleTaskStatus.handle(adminPushTask, taskRound);
        }
    }

    private Long getAdminPushTaskRound(AdminPushTask adminPushTask) {
        if (CycleTypeEnum.WEEK.getType() == adminPushTask.getCycleType()) {
            return (adminPushTask.getActionTime() - adminPushTask.getFirstActionTime()) / adminPushTaskService.WEEK_MILLIS + 1L;
        } else if (CycleTypeEnum.DAY.getType() == adminPushTask.getCycleType()) {
            return (adminPushTask.getActionTime() - adminPushTask.getFirstActionTime()) / adminPushTaskService.DAY_MILLIS + 1L;
        } else {
            return 1L;
        }
    }

    /**
     * 测试发送
     */
    public void sendAdminTestPush(SendAdminTestPushRequest request) {
        if (request.getUserIdList().size() > 100) {
            //测试发送不允许过100
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR, "Test push user more than 100!");
        }
        long orgId = request.getHeader().getOrgId();
        List<AppPushDevice> list = appPushDeviceService.getPushDeviceByUserIds(orgId, request.getUserIdList());
        Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> appPushDeviceMap = handleAppPushDeviceGroup(list);
        //发起测试请求
        appPushDeviceMap.forEach((key, value) -> {
            //分组发送,自定义reqOrderId,当前的tokens
            String reqOrderId = "test_" + RandomStringUtils.randomAlphanumeric(8);
            List<String> tokenList = value.stream().map(AppPushDeviceSimple::getToken).collect(Collectors.toList());
            SendPushRequest sendPushRequest = SendPushRequest.newBuilder()
                    .addAllPushToken(tokenList)
                    .setOrgId(orgId)
                    .setAppId(key.getAppId())
                    .setAppChannel(key.getAppChannel())
                    .setPushChannel(key.getThirdPushType())
                    .setDeviceType(SendPushRequest.DeviceType.valueOf(key.getDeviceType().toUpperCase()))
                    .setReqOrderId(reqOrderId)
                    .setTitle(request.getPushTitle())
                    .setContent(request.getPushContent())
                    .setSummary(request.getPushSummary())
                    .setUrl(request.getPushUrl())
                    .setUrlType(request.getUrlType())
                    .build();
            log.info("test push request! reqOrderId:{},language:{},startId:{},endId{},size:{}", reqOrderId, key.getLanguage(), value.get(0).getId(), value.get(value.size() - 1).getId(), value.size());
            MessageReply messageReply = grpcPushService.sendPush(sendPushRequest);
            if (!messageReply.getSuccess()) {
                //测试请求直接终止
                log.error("test push request fail!{},{},{}", orgId, reqOrderId, messageReply.getMessage());
            }
        });
    }

    /**
     * 对设备进行发送分组
     */
    Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> handleAppPushDeviceGroup(List<AppPushDevice> list) {
        //过滤关闭推送的设备
        list = list.stream().filter(appPushDevice -> appPushDevice.getPushLimit() == 0 && !"jpush".equals(appPushDevice.getThirdPushType())).collect(Collectors.toList());
        //对数据分组
        Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> appPushDeviceMap = new HashMap<>(8);
        list.forEach(appPushDevice -> {
            AppPushDeviceIndex appPushDeviceIndex = new AppPushDeviceIndex(appPushDevice.getAppId(), appPushDevice.getAppChannel(),
                    appPushDevice.getThirdPushType(), appPushDevice.getDeviceType(), appPushDevice.getLanguage());
            List<AppPushDeviceSimple> deviceSimpleList;
            AppPushDeviceSimple deviceSimple = new AppPushDeviceSimple(appPushDevice.getId(), appPushDevice.getDeviceToken());
            if (appPushDeviceMap.containsKey(appPushDeviceIndex)) {
                deviceSimpleList = appPushDeviceMap.get(appPushDeviceIndex);
            } else {
                deviceSimpleList = new ArrayList<>();
                appPushDeviceMap.put(appPushDeviceIndex, deviceSimpleList);
            }
            deviceSimpleList.add(deviceSimple);
        });
        return appPushDeviceMap;
    }

    /**
     * 根据推送范围类型获取推送用户
     */
    private Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> getPushUserGroup(AdminPushTask adminPushTask) throws ExecutionException {
        Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> appPushDeviceMap;
        if (adminPushTask.getRangeType() == 9) {
            List<Long> userIds = adminPushTaskService.queryUserIdsByTaskId(adminPushTask.getTaskId());
            List<AppPushDevice> deviceList = appPushDeviceService.getPushDeviceByUserIds(adminPushTask.getOrgId(), userIds);
            //排序
            if (deviceList.size() > 1) {
                deviceList = deviceList.stream().sorted(Comparator.comparingLong(AppPushDevice::getId)).collect(Collectors.toList());
            }
            appPushDeviceMap = handleAppPushDeviceGroup(deviceList);
        } else {
            appPushDeviceMap = orgAppPushDeviceMap.get(adminPushTask.getOrgId());
        }
        //过滤掉语言为空的用户，且本任务没有指定默认语言
        if (StringUtils.isBlank(adminPushTask.getDefaultLanguage())) {
            appPushDeviceMap = appPushDeviceMap.entrySet().stream()
                    .filter(entry -> StringUtils.isNoneBlank(entry.getKey().getLanguage()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return appPushDeviceMap;
    }

    private void handleFinishStatus(AdminPushTask adminPushTask, Long taskRound, AppPushTaskCache appPushTaskCache) {
        if (adminPushTask.getCycleType() <= 2) {
            int finishResult = adminPushTaskService.updateAdminPushTaskFinishStatus(adminPushTask, "success");
            if (finishResult > 0) {
                updateRedisPushTaskCache(adminPushTask, taskRound, appPushTaskCache, PushTaskStatusEnum.FINISH);
            }
        } else {
            int periodFinishResult = adminPushTaskService.updateAdminPushTaskPeriodFinishStatus(adminPushTask, "success");
            if (periodFinishResult > 0) {
                updateRedisPushTaskCache(adminPushTask, taskRound, appPushTaskCache, PushTaskStatusEnum.PERIOD_FINISH);
            }
        }
    }

    /**
     * 判断是否需要更新redis锁的失效时间(如果超过30s既更新)
     * 避免不必要的频繁更新
     */
    private long checkUpdateRedisLockExpire(long lockTime, String lockKey, String randomStr) {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lockTime >= 30 * 1000) {
            redisStringLockUtil.updateExpire(lockKey, 60, randomStr);
            lockTime = currentTimeMillis;
        }
        return lockTime;
    }

    public class PushingStatusTaskHandle implements IHandleTaskStatus {

        @Override
        public void handle(AdminPushTask adminPushTask, Long taskRound) {
            //处理正在进行中的任务(基本属于各种未正常执行完成的任务)
            log.info("handle admin push task!{},{}", adminPushTask.getTaskId(), PushTaskStatusEnum.PUSHING);
            String randomStr = RandomStringUtils.randomAlphanumeric(8);
            String lockKey = TASK_LOCK_PRE + adminPushTask.getTaskId();
            if (redisStringLockUtil.lock(lockKey, 60, 1, 500L, randomStr)) {
                try {
                    long lockTime = System.currentTimeMillis();
                    AppPushTaskCache appPushTaskCache = pushTaskRedisTemplate.opsForValue().get(adminPushTask.getTaskId());
                    if (appPushTaskCache == null || appPushTaskCache.getStatus() == PushTaskStatusEnum.PUSHING.getStatus()) {
                        try {
                            Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> appPushDeviceMap = getPushUserGroup(adminPushTask);
                            //过滤已发送的数据
                            List<AdminPushTaskDetail> pushTaskDetails = adminPushTaskDetailService.queryPushTaskDetailByTask(adminPushTask.getTaskId(), taskRound);
                            Map<AppPushDeviceIndex, Long> endIdMap = new HashMap<>(8);
                            if (pushTaskDetails.size() > 0) {
                                //获取发送成功的截止id
                                pushTaskDetails.forEach(adminPushTaskDetail -> {
                                    AppPushDeviceIndex appPushDeviceIndex = new AppPushDeviceIndex(adminPushTaskDetail.getAppId(), adminPushTaskDetail.getAppChannel(),
                                            adminPushTaskDetail.getThirdPushType(), adminPushTaskDetail.getDeviceType(), adminPushTaskDetail.getLanguage());
                                    Long oldEndId = endIdMap.getOrDefault(appPushDeviceIndex, 0L);
                                    if (adminPushTaskDetail.getEndId() > oldEndId) {
                                        endIdMap.put(appPushDeviceIndex, adminPushTaskDetail.getEndId());
                                    }
                                });
                            }
                            lockTime = checkUpdateRedisLockExpire(lockTime, lockKey, randomStr);
                            for (Map.Entry<AppPushDeviceIndex, List<AppPushDeviceSimple>> entry : appPushDeviceMap.entrySet()) {
                                AppPushDeviceIndex index = entry.getKey();
                                List<AppPushDeviceSimple> simpleList;
                                Long endId = endIdMap.getOrDefault(index, 0L);
                                if (endId > 0) {
                                    //基于二分法查询对应的索引key，不应该找不到
                                    int endIdIndex = Collections.binarySearch(entry.getValue(), new AppPushDeviceSimple(endId, ""));
                                    if (endIdIndex < 0) {
                                        log.error("not found endId push device!{}", endId);
                                        //暴力低效率的过滤
                                        simpleList = entry.getValue().stream().filter(simple -> simple.getId() > endId).collect(Collectors.toList());
                                    } else {
                                        simpleList = entry.getValue().subList(endIdIndex, entry.getValue().size() - 1);
                                    }
                                } else {
                                    simpleList = entry.getValue();
                                }
                                sendPushRequest(adminPushTask, taskRound, index, simpleList);
                                lockTime = checkUpdateRedisLockExpire(lockTime, lockKey, randomStr);
                            }
                            //更新推送状态为已发送完成或本轮发送完成
                            handleFinishStatus(adminPushTask, taskRound, appPushTaskCache);
                        } catch (Exception e) {
                            updateRedisPushTaskRetry(adminPushTask, taskRound, appPushTaskCache);
                            log.error("update admin push task error!{},{}", adminPushTask.getTaskId(), adminPushTask.getStatus(), e);
                        }
                    }
                } catch (Exception e) {
                    log.error("update admin push task error!{},{}", adminPushTask.getTaskId(), PushTaskStatusEnum.PUSHING, e);
                } finally {
                    //释放缓存
                    taskExistCache.put(adminPushTask.getTaskId() + "_" + taskRound, false);
                    redisStringLockUtil.releaseLock(lockKey, randomStr);
                }
            } else {
                log.info("give up update admin push task!{},{}", adminPushTask.getTaskId(), PushTaskStatusEnum.PUSHING);
            }
        }
    }

    public class PreStartTaskStatusHandle implements IHandleTaskStatus {

        @Override
        public void handle(AdminPushTask adminPushTask, Long taskRound) {
            //处理初次执行及周期执行
            log.info("handle admin push task!{},{}", adminPushTask.getTaskId(), adminPushTask.getStatus());
            String randomStr = RandomStringUtils.randomAlphanumeric(8);
            String lockKey = TASK_LOCK_PRE + adminPushTask.getTaskId();
            if (redisStringLockUtil.lock(lockKey, 60, 1, 500L, randomStr)) {
                try {
                    long lockTime = System.currentTimeMillis();
                    AppPushTaskCache appPushTaskCache = pushTaskRedisTemplate.opsForValue().get(adminPushTask.getTaskId());
                    if (appPushTaskCache == null || adminPushTask.getStatus().equals(appPushTaskCache.getStatus())) {
                        //更新正在进行的状态
                        Long curTime = System.currentTimeMillis();
                        int taskResult = adminPushTaskService.updateAdminPushTaskPushingStatus(adminPushTask, curTime, "pushing");
                        if (taskResult > 0) {
                            //mysql修改成功更新,对应的本地task更新时间
                            adminPushTask.setUpdated(curTime);
                            updateRedisPushTaskCache(adminPushTask, taskRound, appPushTaskCache, PushTaskStatusEnum.PUSHING);
                            //执行推送任务
                            Map<AppPushDeviceIndex, List<AppPushDeviceSimple>> appPushDeviceMap = getPushUserGroup(adminPushTask);
                            //更新或者初始化更新mysql统计
                            int sendCount = appPushDeviceMap.values().stream().mapToInt(List::size).sum();
                            appPushStatisticsService.initPushAdminStatisticNum(adminPushTask, taskRound, sendCount);
                            lockTime = checkUpdateRedisLockExpire(lockTime, lockKey, randomStr);
                            for (Map.Entry<AppPushDeviceIndex, List<AppPushDeviceSimple>> entry : appPushDeviceMap.entrySet()) {
                                AppPushDeviceIndex index = entry.getKey();
                                List<AppPushDeviceSimple> simpleList = entry.getValue();
                                sendPushRequest(adminPushTask, taskRound, index, simpleList);
                                lockTime = checkUpdateRedisLockExpire(lockTime, lockKey, randomStr);
                            }
                            //更新推送状态为已发送完成或本轮发送完成
                            handleFinishStatus(adminPushTask, taskRound, appPushTaskCache);
                        }
                    }
                } catch (Exception e) {
                    log.error("update admin push task cancel status error!{}", adminPushTask.getTaskId(), e);
                } finally {
                    //释放缓存
                    taskExistCache.put(adminPushTask.getTaskId() + "_" + taskRound, false);
                    redisStringLockUtil.releaseLock(lockKey, randomStr);
                }
            } else {
                log.info("give up update admin push task!{},{}", adminPushTask.getTaskId(), adminPushTask.getStatus());
            }
        }
    }
}
