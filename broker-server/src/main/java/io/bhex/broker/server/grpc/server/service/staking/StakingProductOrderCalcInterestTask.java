package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.broker.grpc.staking.StakingProductRebateStatus;
import io.bhex.broker.grpc.staking.StakingProductType;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 理财项目:定期、锁仓 利息计算 Task Scheduled
 *
 * @author songxd
 * @date 2020-08-01
 */
@Slf4j
@Service
public class StakingProductOrderCalcInterestTask {

    @Resource
    private StakingProductRebateMapper productRebateMapper;

    @Resource StakingProductPermissionMapper stakingProductPermissionMapper;

    @Resource
    private StakingProductTimeStrategy stakingProductTimeStrategy;

    @Resource
    private StakingProductLockedPositionStrategy stakingProductLockedPositionStrategy;

    @Resource
    private StakingProductStrategyService stakingProductStrategyService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    /**
     * (定期+锁仓)派息计算
     * TODO 0 0/30 * * * ?
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void calcInterestTask() {
        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if(CollectionUtils.isEmpty(listStakingProductPermission)){
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());

        Long currentTime = System.currentTimeMillis();
        // 获取整点时间
        Long todayStartMilliseconds = currentTime - currentTime % (3600 * 1000);

        for (long orgId: orgIdList) {
            // lock
            String lockKey = String.format(BrokerLockKeys.STAKING_CALC_TIME_INTEREST_LOCK_KEY, orgId);
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.STAKING_CALC_TIME_INTEREST_LOCK_KEY_EXPIRE);
            if (!lock) {
                //log.info("staking calc interest get lock failed:{}", System.currentTimeMillis());
                continue;
            }

            try {
                // 获取今天的派息设置
                List<StakingProductRebate> productRebateList = productRebateMapper.listCalcInterestTask(orgId, todayStartMilliseconds
                        , StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);

                if (CollectionUtils.isEmpty(productRebateList)) {
                    continue;
                }

                StakingProduct stakingProduct;

                for (StakingProductRebate productRebate : productRebateList) {
                    // 如果没有设置利率或派息金额，则记录报警信息
                    if (productRebate.getRebateRate().compareTo(BigDecimal.ZERO) <= 0
                            && productRebate.getRebateAmount().compareTo(BigDecimal.ZERO) <= 0) {
                        productRebateMapper.updateStatus(productRebate.getOrgId(), productRebate.getId()
                                , StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE, System.currentTimeMillis());
                        log.warn("staking product rebate not set rebate rate and rebate amount,productRebate:{}", productRebate.toString());
                        continue;
                    }

                    stakingProduct = stakingProductStrategyService.getStakingProduct(productRebate.getOrgId(),productRebate.getProductId());
                    if(stakingProduct == null){
                        continue;
                    }

                    if (stakingProduct.getType() == StakingProductType.FI_TIME_VALUE) {
                        stakingProductTimeStrategy.calcInterest(productRebate, stakingProduct, currentTime);
                    } else if (stakingProduct.getType() == StakingProductType.LOCK_POSITION_VALUE) {
                        stakingProductLockedPositionStrategy.calcInterest(productRebate, stakingProduct, currentTime);
                    }
                }
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }
}
