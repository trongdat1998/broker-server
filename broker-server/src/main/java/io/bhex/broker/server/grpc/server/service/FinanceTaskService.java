package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.FinancePurchaseStatus;
import io.bhex.broker.server.domain.FinanceRecordType;
import io.bhex.broker.server.domain.FinanceRedeemStatus;
import io.bhex.broker.server.primary.mapper.FinanceRecordMapper;
import io.bhex.broker.server.model.FinanceRecord;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.List;

@Slf4j
@Service
@Deprecated(since = "new version(staking current product) online")
public class FinanceTaskService {

    private static final int PURCHASE_EXCEPTION_TASK_LIMIT = 100;
    private static final int REDEEM_TRANSFER_TASK_LIMIT = 100;

    private static final int INTEREST_DAILY_TASK_MAX_RETRY_TIMES = 2;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private FinanceRedeemService financeRedeemService;

    @Resource
    private FinancePurchaseService financePurchaseService;

    @Resource
    private FinanceWalletService financeWalletService;

    @Resource
    private FinanceInterestService financeInterestService;

    @Resource
    private FinanceRecordMapper financeRecordMapper;

    /*
     * Warning!!! 今日事，今日毕。系统暂时不支持计算隔天的定时任务。谨记谨记！！！
     */
    // @Scheduled(cron = "0 0 0,2,4,6,8,10 * * ?")
    public void dailyInterestTask() {
        try {
            // 每日任务的锁会锁定1小时，1小时后自动失效，该任务不会自动解锁，默认限制1小时只能执行一次，该任务理论上不会出现重复出现的
            boolean lock = RedisLockUtils.tryLock(redisTemplate, BrokerLockKeys.FINANCE_DAILY_TASK_LOCK_KEY, BrokerLockKeys.FINANCE_DAILY_TASK_LOCK_EXPIRE);
            if (!lock) {
                log.warn(" **** Finance **** interestDailyTask cannot get lock");
                return;
            }

            // 1、计算昨日利息
            financeWalletService.financeInterestTask();

            // 2、计算七日年化。这个任务重复执行也没啥问题，重复计算几次相同的数据而已
            DateTime dateTime = new DateTime().minusDays(1);
            String statisticsTime = new SimpleDateFormat("yyyyMMdd").format(dateTime.toDate());
            financeInterestService.computeFinanceProductSevenYearRate(statisticsTime);

            // 6、发放收益（是否自动？）
            // financeInterestService.grantFinanceInterest(statisticsTime, true);

        } catch (Exception e) {
            log.error("**** Finance **** interestDailyTask failed, catch exception, retry!!!", e);
        }
    }

    /**
     * 申购异常处理任务
     */
    // @Scheduled(cron = "30 0/2 * * * ?")
    public void purchaseTask() {
        boolean lock = RedisLockUtils.tryLock(redisTemplate, BrokerLockKeys.FINANCE_PURCHASE_EXCEPTION_LOCK_KEY, BrokerLockKeys.FINANCE_PURCHASE_EXCEPTION_LOCK_EXPIRE);
        if (!lock) {
            return;
        }
        try {
            List<FinanceRecord> recordList = financeRecordMapper.getFinanceRecordByTypeAndStatus(FinanceRecordType.PURCHASE.type(), FinancePurchaseStatus.WAITING.getStatus(), PURCHASE_EXCEPTION_TASK_LIMIT);
            for (FinanceRecord record : recordList) {
                try {
                    financePurchaseService.purchaseExecute(record);
                } catch (Exception e) {
                    log.error(" purchaseTask execute catch a exception, record:[{}]", JsonUtil.defaultGson().toJson(record), e);
                }
            }
        } catch (Exception e) {
            log.error(" purchaseExceptionTask catch exception.", e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, BrokerLockKeys.FINANCE_PURCHASE_EXCEPTION_LOCK_KEY);
        }
    }

    /**
     * 赎回任务处理
     */
    // @Scheduled(cron = "30 0/5 * * * ?")
    public void redeemTask() {
        boolean lock = RedisLockUtils.tryLock(redisTemplate, BrokerLockKeys.FINANCE_REDEEM_TRANSFER_LOCK_KEY, BrokerLockKeys.FINANCE_REDEEM_TRANSFER_LOCK_EXPIRE);
        if (!lock) {
            return;
        }
        try {
            // 获取等待赎回的记录
            List<FinanceRecord> recordList = financeRecordMapper.getFinanceRecordByTypeAndStatus(FinanceRecordType.REDEEM.type(), FinanceRedeemStatus.WAITING.getStatus(), REDEEM_TRANSFER_TASK_LIMIT);
            for (FinanceRecord redeemRecord : recordList) {
                try {
                    // 执行赎回操作
                    financeRedeemService.redeemExecute(redeemRecord);
                } catch (Exception e) {
                    log.error(" redeemTask execute catch a exception, record: [{}]", JsonUtil.defaultGson().toJson(redeemRecord), e);
                }
            }
        } catch (Exception e) {
            log.error("redeemTransferTask exception:", e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, BrokerLockKeys.FINANCE_REDEEM_TRANSFER_LOCK_KEY);
        }
    }

}
