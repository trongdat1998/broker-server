package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.bhex.broker.server.model.*;
import io.bhex.broker.server.util.BaseReqUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.account.BatchTransferItem;
import io.bhex.base.account.BatchTransferRequest;
import io.bhex.base.account.BatchTransferResponse;
import io.bhex.base.account.BusinessSubject;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.invite.TestInviteFeeBackResponse;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.InviteActivityStatus;
import io.bhex.broker.server.domain.InviteActivityType;
import io.bhex.broker.server.domain.InviteAutoTransferEnum;
import io.bhex.broker.server.domain.InviteBonusRecordStatus;
import io.bhex.broker.server.domain.InviteGrantStatus;
import io.bhex.broker.server.domain.InviteStatisticsRecordStatus;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.client.service.GrpcCommissionService;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.InviteActivityMapper;
import io.bhex.broker.server.primary.mapper.InviteBonusRecordMapper;
import io.bhex.broker.server.primary.mapper.InviteDailyTaskMapper;
import io.bhex.broker.server.primary.mapper.InviteStatisticsRecordMapper;
import io.bhex.broker.server.util.PageUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

@Slf4j
@Service
@EnableScheduling
public class InviteTaskService {

    @Autowired
    InviteTaskService inviteTaskService;

    @Autowired
    InviteService inviteService;

    @Resource
    InviteBonusRecordMapper inviteBonusRecordMapper;

    @Resource
    InviteActivityMapper inviteActivityMapper;

    @Resource
    InviteDailyTaskMapper inviteDailyTaskMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Autowired
    GrpcBatchTransferService grpcBatchTransferService;

    @Autowired
    GrpcCommissionService grpcCommissionService;

    static ExecutorService executorService = Executors.newCachedThreadPool();

    @Resource
    AccountMapper accountMapper;

    @Resource
    InviteStatisticsRecordMapper inviteStatisticsRecordMapper;

    /**
     * 后台功能  生成邀请返佣 用户获得记录
     */
    public int generateAdminInviteBonusRecord(Long orgId, Long time) {
        InviteActivity activityCondition = InviteActivity.builder()
                .orgId(orgId)
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .build();
        InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
        if (activity == null || activity.getStatus().equals(InviteActivityStatus.OFFLINE.getStatus())) {
            log.info(" generateAdminInviteBonusRecord failed : no activity : orgId:{} time:{}.", orgId, time);
            return -1;
        }

        List<InviteBonusRecord> recordList = inviteBonusRecordMapper.selectByOrgIdAndStatisticsTime(orgId, time, 0, 1);
        if (!CollectionUtils.isEmpty(recordList)) {
            log.info(" generateAdminInviteBonusRecord failed : record already have! : orgId:{} time:{}.", orgId, time);
            return -3;
        }

        String lockKey = String.format(BrokerLockKeys.INVITE_GENERATE_RECROD_LOCK_KEY, orgId, time);
        // 拿个锁啊
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.INVITE_GENERATE_RECROD_LOCK_EXPRIE);
        if (!lock) {
            log.info(" generateAdminInviteBonusRecord failed : other thread execute : orgId:{} time:{}.", orgId, time);
            return -2;
        }

        try {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        inviteDailyTaskMapper.updateInviteDailyTaskChangeStatusToProcessing(orgId, time);
                        //生成返佣任务
                        inviteService.createInviteBonusRecord(activity, time);
                    } catch (Exception ex) {
                        if (inviteDailyTaskMapper.updateInviteDailyTaskChangeStatusToFail(orgId, time) != 1) {
                            log.error("updateInviteDailyTaskChangeStatusToFail fail !!! orgId {}, time {}", orgId, time);
                        }
                        log.error("createInviteBonusRecord fail !!! orgId {}", activity.getOrgId(), ex);
                    }
                    // 生成统计记录
                    generateInviteFeebackStatisticsRecrod(activity, time);
                }
            });
        } catch (Exception e) {
            log.error(" generateAdminInviteBonusRecord exception:{}", orgId, time);
        }

        return 1;
    }


    /**
     * 后台功能  邀请返佣记录 手动发放
     */
    public int executeAdminGrantInviteBonus(Long orgId, Long time) {
        InviteActivity activityCondition = InviteActivity.builder()
                .orgId(orgId)
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .build();

        InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
        if (activity == null || activity.getStatus().equals(InviteActivityStatus.OFFLINE.getStatus())) {
            log.info(" executeAdminGrantInviteBonus failed : no activity : orgId:{} time:{}.", orgId, time);
            return -1;
        }

        String lockKey = String.format(BrokerLockKeys.INVITE_EXECUTE_RECROD_LOCK_KEY, orgId, time);
        // 拿个锁啊
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.INVITE_EXECUTE_RECROD_LOCK_EXPRIE);
        if (!lock) {
            log.info(" executeAdminGrantInviteBonus failed : other thread execute : orgId:{} time:{}.", orgId, time);
            return -2;
        }

        try {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    manualExecuteGrantInviteBonus(activity, time);
                }
            });
        } catch (Exception e) {
            log.error(" generateAdminInviteBonusRecord exception:{}", orgId, time);
        }

        return 0;
    }

    /**
     * 每日任务 补偿自动发放佣金失败任务
     */
    @Scheduled(cron = "0 0 2,3,5,7,9 ? * *")
    public void grantInviteBonusTask() {
        Date yesterday = DateUtils.addDays(new Date(), -1);
        long yesterdayTime = Long.valueOf(DateFormatUtils.format(yesterday, "yyyyMMdd"));

        Example example = Example.builder(InviteActivity.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("autoTransfer", 1)
                .andEqualTo("status", 1);

        List<InviteActivity> autoTransferActivities = inviteActivityMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(autoTransferActivities)) {
            return;
        }
        for(InviteActivity inviteActivity : autoTransferActivities) {
            InviteDailyTask task = inviteService.getInviteDailyTask(inviteActivity.getOrgId(), yesterdayTime);
            if (task == null || (task.getGrantStatus() != InviteGrantStatus.WAITING.value() && task.getGrantStatus() != InviteGrantStatus.PART_FINISHED.value())) {
                continue;
            }
            String lockKey = String.format(BrokerLockKeys.INVITE_EXECUTE_RECROD_LOCK_KEY, inviteActivity.getOrgId(), yesterdayTime);
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.INVITE_EXECUTE_RECROD_LOCK_EXPRIE);
            if (!lock) {
                log.info(" executeAdminGrantInviteBonus failed : other thread execute : orgId:{} time:{}.", inviteActivity.getOrgId(), yesterdayTime);
                continue;
            }
            try {
                executorService.execute(() -> manualExecuteGrantInviteBonus(inviteActivity, yesterdayTime));
            } catch (Exception e) {
                log.error(" manualExecuteGrantInviteBonus exception:{}", inviteActivity.getOrgId(), yesterdayTime);
            }
        };


    }

    /**
     * 手动发放邀请返佣（根据生成的邀请返佣记录进行转账）
     */
    public void manualExecuteGrantInviteBonus(InviteActivity activity, Long time) {
        InviteDailyTask task = inviteService.getInviteDailyTask(activity.getOrgId(), time);
        if (task == null || (task.getGrantStatus() != InviteGrantStatus.WAITING.value() && task.getGrantStatus() != InviteGrantStatus.PART_FINISHED.value())) {
            log.error("manualExecuteGrantInviteBonus execute failed : time:{} act:{} ", time, JsonUtil.defaultGson().toJson(activity));
            return;
        }
        // 修改为执行中
        task.setGrantStatus(InviteGrantStatus.EXECUTING.value());
        task.setUpdatedAt(new Date(System.currentTimeMillis()));
        inviteDailyTaskMapper.updateByPrimaryKeySelective(task);

        int count = 0;
        int partExecute = 0;
        long lastId = 0;
        int pageSize = 1000; //大部分org返佣记录在200条以内
        while (true) {
            List<InviteBonusRecord> recordList = inviteBonusRecordMapper
                    .selectByOrgIdAndStatusAndStatisticsTime(activity.getOrgId(),
                            InviteBonusRecordStatus.WAITING.value(), time, 0, pageSize, lastId);
            if (CollectionUtils.isEmpty(recordList)) {
                break;
            }

            lastId = recordList.get(recordList.size() - 1).getId();
            Map<String, List<InviteBonusRecord>> tokenInviteBonusRecordGroup = recordList.stream()
                    .collect(Collectors.groupingBy(InviteBonusRecord::getToken));
            for (String token : tokenInviteBonusRecordGroup.keySet()) {
                try {
                    for (InviteBonusRecord record : tokenInviteBonusRecordGroup.get(token)) {
                        Account account = accountMapper.getMainAccount(record.getOrgId(), record.getUserId());
                        if (account == null) {
                            continue;
                        }

                        // 构建转账请求
                        BatchTransferItem transferItem = BatchTransferItem.newBuilder()
                                .setTargetAccountId(account.getAccountId())
                                .setTargetOrgId(activity.getOrgId())
                                .setAmount(record.getBonusAmount() == null ? "0" : record.getBonusAmount().toString())
                                .setTokenId(record.getToken())
                                .setTargetAccountType(io.bhex.base.account.AccountType.GENERAL_ACCOUNT)
                                .setSubject(BusinessSubject.INVITATION_REFERRAL_BONUS)
                                .build();
                        BatchTransferRequest transferRequest = BatchTransferRequest.newBuilder()
                                .setBaseRequest(BaseReqUtil.getBaseRequest(activity.getOrgId()))
                                .setClientTransferId(record.getTransferId())
                                .setSourceOrgId(activity.getOrgId())
                                .setSourceAccountType(io.bhex.base.account.AccountType.OPERATION_ACCOUNT)
                                .addTransferTo(transferItem)
                                .setSubject(BusinessSubject.INVITATION_REFERRAL_BONUS)
                                .build();
                        log.info("BatchTransferRequest:{} ", transferRequest);
                        BatchTransferResponse batchTransferResponse = grpcBatchTransferService.batchTransfer(transferRequest);
                        if (batchTransferResponse.getErrorCode() == 0) {
                            record.setStatus(InviteBonusRecordStatus.FINISHED.value());
                            record.setUpdatedAt(new Date(System.currentTimeMillis()));
                            inviteBonusRecordMapper.updateByPrimaryKey(record);

                            int row = inviteStatisticsRecordMapper.incrTransferAmount(activity.getOrgId(), record.getActType(),
                                    record.getStatisticsTime(), record.getToken(), record.getBonusAmount());
                            if (row != 1) {
                                log.error("ALERT incrTransferAmount failed org:{}, actType:{}, statisticsTime:{}, token:{} ",
                                        activity.getOrgId(), record.getActType(), record.getStatisticsTime(), record.getToken());
                            }
                        }
                        log.info(" invite fee back userId:{} token:{} amount:{}  BatchTransferResponse :{}",
                                record.getUserId(), record.getToken(), record.getBonusAmount(), batchTransferResponse);
                        count++;
                    }
                } catch (Exception e) { //一个token的转账失败 转向下一组token
                    log.error(" manualExecuteGrantInviteBonus execption token:{} task:{}", token, JsonUtil.defaultGson().toJson(task), e);
                    partExecute = 1;
                }
            }

            if (recordList.size() < pageSize) {
                break;
            }

        }
        //如果总条数 等于总发放数则是FINISHED 否则就是PART_FINISHED
        int total = inviteBonusRecordMapper.selectCountByOrgIdAndStatisticsTime(task.getOrgId(), task.getStatisticsTime());
        int successCount = inviteBonusRecordMapper.selectCountSuccessByOrgIdAndStatisticsTime(task.getOrgId(), task.getStatisticsTime());

        InviteGrantStatus grantStatus;
        if (total == successCount) {
            grantStatus = InviteGrantStatus.FINISHED;
        } else {
            grantStatus = InviteGrantStatus.PART_FINISHED;
        }

        if (total == 0 && successCount == 0) {
            grantStatus = InviteGrantStatus.WAITING;
        }
        // 修改为发放完成
        task.setGrantStatus(grantStatus.value());
        task.setUpdatedAt(new Date(System.currentTimeMillis()));
        inviteDailyTaskMapper.updateByPrimaryKeySelective(task);

        log.info(" executeGrantInviteBonus finished: orgId:{} time:{} execute count:{} status:{}", activity.getOrgId(), time, count, grantStatus);
    }

    public TestInviteFeeBackResponse testInviteFeeBack(Long orgId, Long time) {
        String lockKey = String.format(BrokerLockKeys.INVITE_TEST_DAILY_TASK_LOCK_KEY, orgId, time);
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.INVITE_TEST_DAILY_TASK_LOCK_EXPRIE);
        if (!lock) {
            log.info(" testInviteFeeBack finish : execute lock : {} -->{}.", orgId, time);
            return TestInviteFeeBackResponse.newBuilder().setRet(-100).build();
        }

        try {
            InviteActivity activityCondition = InviteActivity.builder()
                    .orgId(orgId)
                    .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                    .build();

            InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
            if (activity == null) {
                return TestInviteFeeBackResponse.newBuilder()
                        .setRet(BrokerErrorCode.ACTIVITY_INVITE_ACTIVITY_NOT_EXIST.code())
                        .build();
            }

            InviteDailyTask task = inviteService.getInviteDailyTask(activity.getOrgId(), time);
            if (task == null) {
                // 生成每日任务
                InviteDailyTask dailyTask = InviteDailyTask.builder()
                        .orgId(activity.getOrgId())
                        .statisticsTime(time)
                        .totalAmount(0D)
                        .changeStatus(0)
                        .grantStatus(InviteGrantStatus.WAITING.value())
                        .createdAt(new Date(System.currentTimeMillis()))
                        .updatedAt(new Date(System.currentTimeMillis()))
                        .build();
                inviteDailyTaskMapper.insert(dailyTask);
            }

            //邀请返佣记录生成任务
            try {
                inviteService.createInviteBonusRecord(activity, time);
            } catch (Exception ex) {
                if (inviteDailyTaskMapper.updateInviteDailyTaskChangeStatusToFail(orgId, time) != 1) {
                    log.error("updateInviteDailyTaskChangeStatusToFail fail !!! orgId {}, time {}", orgId, time);
                }
                log.error("createInviteBonusRecord fail !!! orgId {}", activity.getOrgId(), ex);
            }

            // 生成统计记录
            generateInviteFeebackStatisticsRecrod(activity, time);

            return TestInviteFeeBackResponse.getDefaultInstance();
        } catch (Exception e) {
            log.error(" testInviteFeeBack execute Exception:{}-->{}", orgId, time, e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }

        return null;
    }

    /**
     * 每日任务 每天2,4,6,8,10点执行 偶尔手续费数据生成会稍晚 多补偿几次(间隔2小时执行1次)
     */
    @Scheduled(cron = "0 0 1,2,4,6,8 ? * *")
    public void dailyTask() {
        Date yesterday = DateUtils.addDays(new Date(), -1);
        long yesterdayTime = Long.valueOf(DateFormatUtils.format(yesterday, "yyyyMMdd"));

        // 拿个锁啊
        boolean lock = RedisLockUtils.tryLock(redisTemplate, BrokerLockKeys.INVITE_DAILY_TASK_LOCK_KEY, BrokerLockKeys.INVITE_DAILY_TASK_LOCK_EXPRIE);
        if (!lock) {
            log.info(" dailyTask finish : other thread execute.");
            return;
        }

        try {
            List<InviteActivity> activityList = inviteActivityMapper.selectAll();
            if (CollectionUtils.isEmpty(activityList)) {
                log.info(" dailyTask finish : no activity! ");
                return;
            }

            for (InviteActivity inviteActivity : activityList) {
                // 新版交易返佣
                if (inviteActivity.getType().equals(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                        && inviteActivity.getStatus().equals(InviteActivityStatus.ONLINE.getStatus())) {
                    InviteDailyTask task = inviteService.getInviteDailyTask(inviteActivity.getOrgId(), yesterdayTime);
                    if (task == null) {
                        // 生成每日任务
                        InviteDailyTask dailyTask = InviteDailyTask.builder()
                                .orgId(inviteActivity.getOrgId())
                                .statisticsTime(yesterdayTime)
                                .totalAmount(0D)
                                .changeStatus(0)
                                .grantStatus(InviteGrantStatus.WAITING.value())
                                .createdAt(new Date(System.currentTimeMillis()))
                                .updatedAt(new Date(System.currentTimeMillis()))
                                .build();
                        inviteDailyTaskMapper.insert(dailyTask);
                    }

                    int bonusRecord = this.inviteBonusRecordMapper.selectCountByOrgIdAndStatisticsTime(inviteActivity.getOrgId(), yesterdayTime);
                    if (bonusRecord > 0) {
                        //bonusRecord > 0标识已经正常生成过了 则不再处理
                        continue;
                    }

                    //邀请返佣记录生成任务
                    try {
                        inviteService.createInviteBonusRecord(inviteActivity, yesterdayTime);
                    } catch (Exception ex) {
                        if (inviteDailyTaskMapper.updateInviteDailyTaskChangeStatusToFail(inviteActivity.getOrgId(), yesterdayTime) != 1) {
                            log.error("updateInviteDailyTaskChangeStatusToFail fail !!! orgId {}, time {}", inviteActivity.getOrgId(), yesterdayTime);
                        }
                        log.error("createInviteBonusRecord fail !!! orgId {}", inviteActivity.getOrgId(), ex);
                    }

                    //生成统计报表
                    generateInviteFeebackStatisticsRecrod(inviteActivity, yesterdayTime);

                    if (inviteActivity.getAutoTransfer() == InviteAutoTransferEnum.AUTO.code()) {
                        //执行自动发放逻辑
                        manualExecuteGrantInviteBonus(inviteActivity, yesterdayTime);
                    }
                }
            }
        } catch (Exception e) {
            log.info("dailyTask exception : {}", yesterdayTime, e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, BrokerLockKeys.INVITE_DAILY_TASK_LOCK_KEY);
        }
    }

    public void manualCompensationCreateBonusRecord(long yesterdayTime) {
        // 拿个锁啊
        boolean lock = RedisLockUtils.tryLock(redisTemplate, BrokerLockKeys.INVITE_DAILY_TASK_LOCK_KEY, BrokerLockKeys.INVITE_DAILY_TASK_LOCK_EXPRIE);
        if (!lock) {
            log.info(" dailyTask finish : other thread execute.");
            return;
        }

        try {
            List<InviteActivity> activityList = inviteActivityMapper.selectAll();
            if (CollectionUtils.isEmpty(activityList)) {
                log.info(" dailyTask finish : no activity! ");
                return;
            }

            for (InviteActivity inviteActivity : activityList) {
                // 新版交易返佣
                if (inviteActivity.getType().equals(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                        && inviteActivity.getStatus().equals(InviteActivityStatus.ONLINE.getStatus())) {
                    InviteDailyTask task = inviteService.getInviteDailyTask(inviteActivity.getOrgId(), yesterdayTime);
                    if (task == null) {
                        // 生成每日任务
                        InviteDailyTask dailyTask = InviteDailyTask.builder()
                                .orgId(inviteActivity.getOrgId())
                                .statisticsTime(yesterdayTime)
                                .totalAmount(0D)
                                .changeStatus(0)
                                .grantStatus(InviteGrantStatus.WAITING.value())
                                .createdAt(new Date(System.currentTimeMillis()))
                                .updatedAt(new Date(System.currentTimeMillis()))
                                .build();
                        inviteDailyTaskMapper.insert(dailyTask);
                    }

                    int bonusRecord = this.inviteBonusRecordMapper.selectCountByOrgIdAndStatisticsTime(inviteActivity.getOrgId(), yesterdayTime);
                    if (bonusRecord > 0) {
                        //bonusRecord > 0标识已经正常生成过了 则不再处理
                        continue;
                    }

                    //邀请返佣记录生成任务
                    try {
                        inviteService.createInviteBonusRecord(inviteActivity, yesterdayTime);
                    } catch (Exception ex) {
                        if (inviteDailyTaskMapper.updateInviteDailyTaskChangeStatusToFail(inviteActivity.getOrgId(), yesterdayTime) != 1) {
                            log.error("updateInviteDailyTaskChangeStatusToFail fail !!! orgId {}, time {}", inviteActivity.getOrgId(), yesterdayTime);
                        }
                        log.error("createInviteBonusRecord fail !!! orgId {}", inviteActivity.getOrgId(), ex);
                    }

                    //生成统计报表
                    generateInviteFeebackStatisticsRecrod(inviteActivity, yesterdayTime);

                    if (inviteActivity.getAutoTransfer() == InviteAutoTransferEnum.AUTO.code()) {
                        //执行自动发放逻辑
                        manualExecuteGrantInviteBonus(inviteActivity, yesterdayTime);
                    }
                }
            }
        } catch (Exception e) {
            log.info("dailyTask exception : {}", yesterdayTime, e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, BrokerLockKeys.INVITE_DAILY_TASK_LOCK_KEY);
        }
    }

    /**
     * 生成邀请返佣 统计记录 （统计每天总共给用户发放 amount 个 token）
     */
    public void generateInviteFeebackStatisticsRecrod(InviteActivity activity, long time) {
        int index = 1;
        int limit = 100;
        Map<String, Double> statisticsMap = Maps.newHashMap();
        while (true) {

            int startIndex = PageUtil.getStartIndex(index, limit);
            List<InviteBonusRecord> recordList = inviteBonusRecordMapper.selectByOrgIdAndStatisticsTime(activity.getOrgId(), time, startIndex, limit);
            if (CollectionUtils.isEmpty(recordList)) {
                break;
            }

            for (InviteBonusRecord record : recordList) {
                Double amount = statisticsMap.get(record.getToken());
                if (amount == null) {
                    amount = record.getBonusAmount();
                } else {
                    amount = amount + record.getBonusAmount();
                }
                statisticsMap.put(record.getToken(), amount);
            }

            index++;
        }

        if (CollectionUtils.isEmpty(statisticsMap)) {
            return;
        }

        List<InviteStatisticsRecord> insertList = Lists.newArrayList();
        List<InviteStatisticsRecord> updateList = Lists.newArrayList();
        for (Map.Entry<String, Double> entry : statisticsMap.entrySet()) {
            String token = entry.getKey();
            Double amount = entry.getValue();
            if (amount == null || amount <= 0) {
                continue;
            }

            InviteStatisticsRecord condition = InviteStatisticsRecord.builder()
                    .orgId(activity.getOrgId())
                    .statisticsTime(time)
                    .token(token)
                    .build();
            InviteStatisticsRecord dbRecord = inviteStatisticsRecordMapper.selectOne(condition);
            if (dbRecord == null) {
                // 构建对象
                insertList.add(InviteStatisticsRecord.builder()
                        .orgId(activity.getOrgId())
                        .inviteType(activity.getType())
                        .statisticsTime(time)
                        .token(token)
                        .amount(amount)
                        .transferAmount(0D)
                        .status(InviteStatisticsRecordStatus.WAITING.getStatus())
                        .createdAt(new Date(System.currentTimeMillis()))
                        .updatedAt(new Date(System.currentTimeMillis()))
                        .build());
            } else {
                dbRecord.setAmount(amount);
                dbRecord.setUpdatedAt(new Date(System.currentTimeMillis()));
                updateList.add(dbRecord);
            }
            log.info(" generateInviteFeebackStatisticsRecrod orgId:{} time:{} ---> {}:{}",
                    activity.getOrgId(), time, token, amount);
        }


        // 入库
        if (!CollectionUtils.isEmpty(insertList)) {
            inviteStatisticsRecordMapper.insertList(insertList);
        }
        if (!CollectionUtils.isEmpty(updateList)) {
            for (InviteStatisticsRecord record : updateList) {
                inviteStatisticsRecordMapper.updateByPrimaryKeySelective(record);
            }
        }
    }

    public TestInviteFeeBackResponse testExecuteFuturesAdminGrantInviteBonus(Long orgId, Long time) {
        try {
            InviteActivity activityCondition = InviteActivity.builder()
                    .orgId(orgId)
                    .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                    .build();

            InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
            if (activity == null) {
                return TestInviteFeeBackResponse.newBuilder()
                        .setRet(BrokerErrorCode.ACTIVITY_INVITE_ACTIVITY_NOT_EXIST.code())
                        .build();
            }

            InviteDailyTask task = inviteService.getInviteDailyTask(activity.getOrgId(), time);
            if (task == null) {
                // 生成每日任务
                InviteDailyTask dailyTask = InviteDailyTask.builder()
                        .orgId(activity.getOrgId())
                        .statisticsTime(time)
                        .totalAmount(0D)
                        .changeStatus(0)
                        .grantStatus(InviteGrantStatus.WAITING.value())
                        .createdAt(new Date(System.currentTimeMillis()))
                        .updatedAt(new Date(System.currentTimeMillis()))
                        .build();
                inviteDailyTaskMapper.insert(dailyTask);
            }

            //邀请返佣记录生成任务
            try {
                inviteService.testCreateInviteBonusRecord(activity, time);
            } catch (Exception ex) {
                log.error("createInviteBonusRecord fail !!! orgId {}", activity.getOrgId(), ex);
            }
            return TestInviteFeeBackResponse.getDefaultInstance();
        } catch (Exception e) {
            log.error(" testInviteFeeBack execute Exception:{}-->{}", orgId, time, e);
        }
        return TestInviteFeeBackResponse.getDefaultInstance();
    }
}