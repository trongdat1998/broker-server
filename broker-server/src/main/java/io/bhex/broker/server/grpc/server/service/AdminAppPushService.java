package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.model.AdminPushTask;
import io.bhex.broker.server.model.AdminPushTaskDetail;
import io.bhex.broker.server.model.AdminPushTaskLocale;
import io.bhex.broker.server.model.AdminPushTaskStatistics;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.push.bo.PushTaskStatusEnum;
import io.bhex.broker.server.push.bo.RedisStringLockUtil;
import io.bhex.broker.server.push.service.AdminPushTaskDetailService;
import io.bhex.broker.server.push.service.AdminPushTaskLocaleService;
import io.bhex.broker.server.push.service.AdminPushTaskService;
import io.bhex.broker.server.push.service.AdminPushTaskStatisticsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author wangsc
 * @description 管理端操作Push
 * @date 2020-07-25 18:16
 */
@Slf4j
@Service
public class AdminAppPushService {

    @Resource
    private AdminPushTaskService adminPushTaskService;

    @Resource
    private AdminPushTaskLocaleService adminPushTaskLocaleService;

    @Resource
    private AdminPushTaskDetailService adminPushTaskDetailService;

    @Resource
    private AdminPushTaskStatisticsService adminPushTaskStatisticsService;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private RedisStringLockUtil redisStringLockUtil;

    @Resource(name = "pushTaskLimitRedisTemplate")
    private RedisTemplate<String, Long> pushTaskLimitRedisTemplate;

    private final String TASK_LOCK_PRE = "PUSH_TASK:";

    private final String ORG_LOCK_PRE = "PUSH_ORG:";

    public void addAdminPushTask(AdminPushTask adminPushTask, List<AdminPushTaskLocale> localeList) {
        //判断是否有权限创建
        String lockKey = ORG_LOCK_PRE + adminPushTask.getOrgId();
        String randomStr = RandomStringUtils.randomAlphanumeric(8);
        if (redisStringLockUtil.lock(lockKey, 30, 2, 1000L, randomStr)) {
            try {
                Long utcDayTime = Instant.now().getEpochSecond() / 86400 * 86400 * 1000;
                String cacheKey = adminPushTask.getOrgId() + "_" + utcDayTime;
                Long taskCount = pushTaskLimitRedisTemplate.opsForValue().get(cacheKey);
                if (taskCount == null) {
                    taskCount = 0L;
                    Boolean result = pushTaskLimitRedisTemplate.opsForValue().setIfAbsent(cacheKey, taskCount, 1L, TimeUnit.DAYS);
                    if (result == null || Boolean.FALSE.equals(result)) {
                        taskCount = pushTaskLimitRedisTemplate.opsForValue().get(cacheKey);
                    }
                }
                long maxTaskCount = 5L;
                BaseConfigInfo configInfo = baseBizConfigService.getOneConfig(adminPushTask.getOrgId(),
                        BaseConfigConstants.APPPUSH_CONFIG_GROUP, "custom.upper.limit");
                if (configInfo != null) {
                    maxTaskCount = Long.parseLong(configInfo.getConfValue());
                }
                if (taskCount == null) {
                    throw new BrokerException(BrokerErrorCode.REQUEST_INVALID, "Redis unknown error");
                } else if(taskCount >= maxTaskCount) {
                    throw new BrokerException(BrokerErrorCode.REQUEST_INVALID, "The number of tasks created today exceeds the upper limit " + maxTaskCount);
                } else {
                    addAdminPushTaskTransactional(adminPushTask, localeList);
                    //不抛异常认为任务添加正常，redis增量+1
                    pushTaskLimitRedisTemplate.opsForValue().increment(cacheKey);
                }
            } finally {
                redisStringLockUtil.releaseLock(lockKey, randomStr);
            }
        } else {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID, "Another task is being created");
        }
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    void addAdminPushTaskTransactional(AdminPushTask adminPushTask, List<AdminPushTaskLocale> localeList) {
        //新增task
        adminPushTaskService.submit(adminPushTask);
        long taskId = adminPushTask.getTaskId();
        //新增关联的国际化
        for (AdminPushTaskLocale taskLocale : localeList) {
            taskLocale.setTaskId(taskId);
            adminPushTaskLocaleService.submit(taskLocale);
        }
    }

    /**
     * 查询完整的task
     */
    public AdminPushTask queryAdminPushTask(Long orgId, Long taskId) {
        return adminPushTaskService.query(orgId, taskId);
    }

    public List<AdminPushTaskLocale> getPushTaskLocaleList(Long taskId) {
        return adminPushTaskLocaleService.getPushTaskLocaleList(taskId);
    }

    public void editAdminPushTask(AdminPushTask adminPushTask, List<AdminPushTaskLocale> localeList) {
        AdminPushTask oldAdminPushTask = adminPushTaskService.query(adminPushTask.getOrgId(), adminPushTask.getTaskId());
        if(oldAdminPushTask == null) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }
        //只要不是最终状态就允许修改
        if (oldAdminPushTask.getStatus() >= PushTaskStatusEnum.FINISH.getStatus()) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID, "Task final status!");
        }
        //1分钟内即将执行任务
        if (oldAdminPushTask.getActionTime() <= System.currentTimeMillis() + 60 * 1000) {
            //抢锁,抢不到放弃修改
            String randomStr = RandomStringUtils.randomAlphanumeric(8);
            String lockKey = TASK_LOCK_PRE + adminPushTask.getTaskId();
            if (redisStringLockUtil.lock(lockKey, 30, 2, 1000L, randomStr)) {
                try {
                    editAdminPushTaskTransactional(adminPushTask, localeList);
                } finally {
                    redisStringLockUtil.releaseLock(lockKey, randomStr);
                }
            } else {
                throw new BrokerException(BrokerErrorCode.REQUEST_INVALID, "Task pushing!");
            }
        } else {
            editAdminPushTaskTransactional(adminPushTask, localeList);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    void editAdminPushTaskTransactional(AdminPushTask adminPushTask, List<AdminPushTaskLocale> localeList) {
        int result = adminPushTaskService.update(adminPushTask);
        if (result > 0) {
            List<AdminPushTaskLocale> oldLocaleList = adminPushTaskLocaleService.getPushTaskLocaleList(adminPushTask.getTaskId());
            //删除旧的
            for (AdminPushTaskLocale locale : oldLocaleList) {
                adminPushTaskLocaleService.removeById(locale.getId());
            }
            //新增新的
            for (AdminPushTaskLocale locale : localeList) {
                locale.setTaskId(adminPushTask.getTaskId());
                adminPushTaskLocaleService.submit(locale);
            }
        } else {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }
    }

    public void cancelAdminPushTask(Long orgId, Long taskId, String text) {
        AdminPushTask adminPushTask = adminPushTaskService.query(orgId, taskId);
        if (adminPushTask == null) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS, "Task does not exist!");
        }
        //只要不是最终状态就允许删除
        if (adminPushTask.getStatus() >= PushTaskStatusEnum.FINISH.getStatus()) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID, "Task final status!");
        }
        //1分钟内即将执行任务
        if (adminPushTask.getActionTime() <= System.currentTimeMillis() + 60 * 1000) {
            //抢锁,抢不到放弃删除
            String randomStr = RandomStringUtils.randomAlphanumeric(8);
            String lockKey = TASK_LOCK_PRE + adminPushTask.getTaskId();
            if (redisStringLockUtil.lock(lockKey, 30, 2, 500L, randomStr)) {
                try {
                    adminPushTaskService.updateAdminPushTaskCancelStatus(adminPushTask, text);
                } finally {
                    redisStringLockUtil.releaseLock(lockKey, randomStr);
                }
            } else {
                throw new BrokerException(BrokerErrorCode.REQUEST_INVALID, "Task pushing!");
            }
        } else {
            adminPushTaskService.updateAdminPushTaskCancelStatus(adminPushTask, text);
        }
    }

    public List<AdminPushTask> queryPushTasks(Long orgId, Long startTime, Long endTime, Long startId, Long endId, Integer limit) {
        return adminPushTaskService.queryPushTasks(orgId, startTime, endTime, startId, endId, limit);
    }

    /**
     * 获取周期最近执行的统计
     * @param taskId
     * @return
     */
    public AdminPushTaskStatistics queryRecently(Long taskId) {
        return adminPushTaskStatisticsService.queryRecently(taskId);
    }

    public List<AdminPushTaskDetail> queryAdminPushTaskSendDetail(Long orgId, Long taskId, Long taskRound, Long startTime, Integer limit) {
        limit = Math.min(limit, 100);
        return adminPushTaskDetailService.queryAdminPushTaskSendDetail(orgId, taskId, taskRound, startTime, limit);
    }

}
