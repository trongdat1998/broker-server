package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.staking.StakingConstant;
import io.bhex.broker.server.domain.staking.StakingRebateAvailableCount;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 理财项目活期利息计算 Task Scheduled
 *
 * @author songxd
 * @date 2020-08-01
 */
@Slf4j
@Service
public class StakingProductCurrentOrderCalcInterestTask {

    @Resource
    private StakingProductRebateMapper productRebateMapper;

    @Resource
    private StakingProductRebateBatchMapper stakingProductRebateBatchMapper;

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    @Resource
    private StakingProductPermissionMapper stakingProductPermissionMapper;

    @Resource
    private StakingProductStrategyService stakingProductStrategyService;

    @Resource
    private StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    private StakingProductRebateDetailMapper stakingProductRebateDetailMapper;

    @Resource
    private StakingAssetMapper stakingAssetMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private ISequenceGenerator<Long> sequenceGenerator;

    /**
     * 活期资产及利息计算
     * <p>
     * 0 0 1 * * ?
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void calcInterestTask() {

        Long currentTime = System.currentTimeMillis();
        // 获取整点时间
        Long todayStartMilliseconds = currentTime - currentTime % (3600 * 1000);


        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if (CollectionUtils.isEmpty(listStakingProductPermission)) {
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());

        for (long orgId : orgIdList) {

            // lock
            String lockKey = String.format(StakingConstant.STAKING_CURRENT_CALC_INTEREST_LOCK, orgId);
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, StakingConstant.STAKING_CURRENT_CALC_INTEREST_LOCK_EXPIRE_MIN);
            if (!lock) {
                return;
            }

            // 获取最新的派息设置
            List<StakingProductRebate> productRebateList = productRebateMapper.listCurrentCalcInterestTask(orgId
                    , todayStartMilliseconds
                    , StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);
            if (CollectionUtils.isEmpty(productRebateList)) {
                continue;
            }

            StakingProduct stakingProduct;

            try {
                for (StakingProductRebate productRebate : productRebateList) {
                    // 如果没有设置利率或派息金额，则记录报警信息
                    if (productRebate.getRebateRate().compareTo(BigDecimal.ZERO) <= 0
                            && productRebate.getRebateAmount().compareTo(BigDecimal.ZERO) <= 0) {
                        productRebateMapper.updateStatus(productRebate.getOrgId(), productRebate.getId()
                                , StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE, System.currentTimeMillis());
                        log.warn("staking current product rebate not set rebate rate and rebate amount: org:{} product:{} rebate:{}"
                                , productRebate.getOrgId(), productRebate.getProductId(), productRebate.getId());
                        continue;
                    }

                    // 1.get staking current product
                    stakingProduct = stakingProductStrategyService.getStakingProduct(productRebate.getOrgId(), productRebate.getProductId());
                    if (stakingProduct == null) {
                        continue;
                    }

                    log.info("staking current calc interest starting({})...", productRebate.getId());
                    stakingProductCurrentStrategy.calcInterest(productRebate, stakingProduct, currentTime);
                }
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    /**
     * 自动归还本金：当项目到期且资产表中还有资产的记录则增加一条还本记录待后台操作
     */
    @Scheduled(cron = "0 0 0/2 * * ?")
    public void returnPrincipal() {
        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if (CollectionUtils.isEmpty(listStakingProductPermission)) {
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());
        long currentTime = System.currentTimeMillis();
        // 获取整点时间
        Long todayStartMilliseconds = currentTime - currentTime % (3600 * 1000);
        for (long orgId : orgIdList) {

            // lock
            String lockKey = String.format(StakingConstant.STAKING_CURRENT_RETURN_PRINCIPAL_LOCK_KEY, orgId);
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.STAKING_CALC_TIME_INTEREST_LOCK_KEY_EXPIRE);
            if (!lock) {
                continue;
            }

            try {
                List<StakingProductRebate> listRebate = productRebateMapper.listCurrentPrincipal(orgId, todayStartMilliseconds, 0);
                if (CollectionUtils.isEmpty(listRebate)) {
                    continue;
                }
                for (StakingProductRebate productRebate : listRebate) {
                    // 检查有没有未完成的订单，订单状态为0
                    int processingOrderCount = stakingProductOrderMapper.countProcessingOfOrder(productRebate.getOrgId(), productRebate.getProductId());
                    if (processingOrderCount == 0) {
                        // 检查该产品有没有未派息记录
                        int noTransferCount = stakingProductRebateDetailMapper.countNoTransfer(productRebate.getOrgId(), productRebate.getProductId());
                        if (noTransferCount == 0) {
                            StakingProduct stakingProduct = stakingProductStrategyService.getStakingProduct(productRebate.getOrgId(), productRebate.getProductId());
                            long maxId = 0;
                            boolean isFirst = true;
                            while (true) {
                                List<StakingAsset> listAssert = stakingAssetMapper.listAsset(productRebate.getOrgId(), productRebate.getProductId()
                                        , maxId, StakingConstant.BATCH_LIST_SIZE);

                                if (CollectionUtils.isEmpty(listAssert)) {
                                    productRebateMapper.updateStatus(productRebate.getOrgId(), productRebate.getId()
                                            , isFirst ? StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS_VALUE : StakingProductRebateStatus.STPD_REBATE_STATUS_CALCED_VALUE
                                            , System.currentTimeMillis());
                                    break;
                                }
                                isFirst = false;

                                for (StakingAsset stakingAsset : listAssert) {
                                    long transferId = sequenceGenerator.getLong();
                                    int lots = 0;
                                    if (stakingProduct.getPerLotAmount().compareTo(BigDecimal.ZERO) > 0) {
                                        lots = stakingAsset.getCurrentAmount().divide(stakingProduct.getPerLotAmount(), 0, RoundingMode.DOWN).intValue();
                                    }
                                    BigDecimal userSumAsset = stakingProductCurrentStrategy.sumUserAsset(stakingAsset.getOrgId(), stakingAsset.getProductId(), stakingAsset.getUserId());
                                    if (userSumAsset == null) {
                                        userSumAsset = BigDecimal.ZERO;
                                    }
                                    // 如果用户累计资产等于用户资产余额则保存赎回还本订单
                                    if (stakingAsset.getCurrentAmount().compareTo(userSumAsset) == 0) {
                                        try {
                                            boolean insertRnt = stakingProductStrategyService.insertRedeemOrderAndReduceAsset(StakingProductOrder.builder()
                                                    .id(transferId)
                                                    .orgId(stakingAsset.getOrgId())
                                                    .userId(stakingAsset.getUserId())
                                                    .accountId(stakingAsset.getAccountId())
                                                    .productId(stakingAsset.getProductId())
                                                    .productType(productRebate.getProductType())
                                                    .transferId(transferId)
                                                    .payLots(lots)
                                                    .payAmount(BigDecimal.ZERO.subtract(stakingAsset.getCurrentAmount()))
                                                    .orderType(StakingProductOrderType.STPD_ORDER_TYPE_REDEEM_PRINCIPAL_VALUE)
                                                    .tokenId(productRebate.getTokenId())
                                                    .takeEffectDate(0L)
                                                    .redemptionDate(currentTime)
                                                    .lastInterestDate(0L)
                                                    .canAutoRenew(0)
                                                    .status(StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE)
                                                    .createdAt(currentTime)
                                                    .updatedAt(currentTime)
                                                    .build());
                                            if(!insertRnt){
                                                log.error("staking returnPrincipal fail,params->{}", productRebate.toString());
                                                return;
                                            }
                                        } catch (Exception ex) {
                                            log.error("staking returnPrincipal error->{},params->{}", ex, productRebate.toString());
                                            return;
                                        }
                                    }
                                }

                                maxId = listAssert.stream().mapToLong(StakingAsset::getId).max().orElse(0L);
                            }
                        }
                    }
                }
            }
            finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    /**
     * 自动派息
     * 0 0 8/1 * * ?
     */
    @Scheduled(cron = "0 30 0,2,4,6,8,10 * * ?")
    public void autoDividendTransfer() {
        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if (CollectionUtils.isEmpty(listStakingProductPermission)) {
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());

        for (long orgId : orgIdList) {
            // lock
            String lockKey = String.format(StakingConstant.STAKING_CURRENT_AUTO_DIVIDEND_LOCK_KEY, orgId);
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.STAKING_CALC_TIME_INTEREST_LOCK_KEY_EXPIRE);
            if (!lock) {
                continue;
            }
            try {
                // 获取最新的派息设置
                List<StakingProductRebate> productRebateList = productRebateMapper.listWaitTransfer(orgId);
                if (CollectionUtils.isEmpty(productRebateList)) {
                    continue;
                }

                for (StakingProductRebate productRebate : productRebateList) {
                    boolean dataVerify = stakingProductCurrentStrategy.dataVerify(productRebate.getOrgId(), productRebate.getProductId());
                    if (dataVerify) {
                        stakingProductCurrentStrategy.dispatchInterest(StakingTransferEvent.builder()
                                .orgId(productRebate.getOrgId())
                                .productId(productRebate.getProductId())
                                .rebateId(productRebate.getId())
                                .rebateType(productRebate.getType())
                                .build());
                    } else {
                        log.error("staking current auto dividend transfer verify fail:{}", productRebate.toString());
                    }
                }
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    /**
     * 自动补充最近7天的派息设置(备用)
     */
    //@Scheduled(cron = "0 0 0 * * ?")
    public void initProductRebate() {
        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if (CollectionUtils.isEmpty(listStakingProductPermission)) {
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());

        for (long orgId : orgIdList) {
            List<StakingRebateAvailableCount> stakingRebateAvailableCountList = productRebateMapper.listCurrentAvailableRebateCount(orgId
                    , StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);
            if (CollectionUtils.isEmpty(stakingRebateAvailableCountList)) {
                return;
            } else {
                for (StakingRebateAvailableCount stakingRebateAvailableCount : stakingRebateAvailableCountList) {
                    if (stakingRebateAvailableCount.getAvailableCount().compareTo(StakingConstant.STAKING_CURRENT_PRODUCT_REBATE_DEFAULT_COUNT) < 0) {
                        StakingProductRebate lastStakingProductRebate = productRebateMapper.getLastAvailableRebate(orgId
                                , stakingRebateAvailableCount.getProductId());
                        if (lastStakingProductRebate != null) {
                            int count = StakingConstant.STAKING_CURRENT_PRODUCT_REBATE_DEFAULT_COUNT - stakingRebateAvailableCount.getAvailableCount();
                            DateTime dtRebateDate = new DateTime(lastStakingProductRebate.getRebateDate());
                            List<StakingProductRebate> stakingProductRebateList = new ArrayList<>();
                            for (int i = 1; i <= count; i++) {
                                StakingProductRebate productRebate = StakingProductRebate.builder()
                                        .orgId(orgId)
                                        .productId(lastStakingProductRebate.getProductId())
                                        .productType(lastStakingProductRebate.getProductType())
                                        .tokenId(lastStakingProductRebate.getTokenId())
                                        .rebateDate(dtRebateDate.plusDays(i).getMillis())
                                        .type(lastStakingProductRebate.getType())
                                        .rebateCalcWay(lastStakingProductRebate.getRebateCalcWay())
                                        .rebateRate(lastStakingProductRebate.getRebateRate())
                                        .rebateAmount(lastStakingProductRebate.getRebateAmount())
                                        .numberOfPeriods(lastStakingProductRebate.getNumberOfPeriods() + i)
                                        .status(StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE)
                                        .createdAt(System.currentTimeMillis())
                                        .updatedAt(System.currentTimeMillis())
                                        .build();
                                stakingProductRebateList.add(productRebate);
                            }
                            if (!CollectionUtils.isEmpty(stakingProductRebateList)) {
                                stakingProductRebateBatchMapper.insertList(stakingProductRebateList);
                                stakingProductRebateList.clear();
                            }
                        }
                    }
                }
            }
        }
    }
}
