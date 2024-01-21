package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.grpc.staking.StakingProductOrderStatus;
import io.bhex.broker.grpc.staking.StakingProductOrderType;
import io.bhex.broker.grpc.staking.StakingProductRebateStatus;
import io.bhex.broker.grpc.staking.StakingProductType;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.staking.StakingConstant;

import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 理财项目:订单补充处理
 *
 * @author songxd
 * @date 2020-08-01
 */
@Slf4j
@Service
public class StakingProductOrderProcessTask {
    @Resource
    private StakingProductOrderMapper productOrderMapper;

    @Resource
    private StakingProductMapper productMapper;

    @Resource
    private StakingProductRebateMapper stakingProductRebateMapper;

    @Resource
    private StakingProductPermissionMapper stakingProductPermissionMapper;

    @Resource
    private StakingProductTimeStrategy stakingProductTimeStrategy;

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    @Resource
    private StakingProductLockedPositionStrategy stakingProductLockedPositionStrategy;

    @Resource
    private StakingAssetMapper stakingAssetMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    /**
     * 未处理申购订单，补充处理
     */
    @Scheduled(cron = "0 0/2 * * * ?")
    public void calcInterestTask() {
        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if(CollectionUtils.isEmpty(listStakingProductPermission)){
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());

        for (long orgId: orgIdList) {
            // lock
            String lockKey = String.format(StakingConstant.STAKING_SUBSCRIBE_TASK_LOCK, orgId);
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, StakingConstant.STAKING_SUBSCRIBE_TASK_LOCK_EXPIRE);
            if (!lock) {
                continue;
            }
            try {
                // 获取遗漏订单-先处理申购订单
                List<StakingProductOrder> orderList = productOrderMapper.select(StakingProductOrder.builder()
                        .orgId(orgId)
                        .orderType(0)
                        .status(StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE)
                        .build());

                if (CollectionUtils.isEmpty(orderList)) {
                    continue;
                }

                Integer rtnCode = 0;

                for (StakingProductOrder productOrder : orderList) {
                    StakingSubscribeEvent event = StakingSubscribeEvent.builder()
                            .orgId(productOrder.getOrgId())
                            .productId(productOrder.getProductId())
                            .orderId(productOrder.getId())
                            .transferId(productOrder.getTransferId())
                            .userId(productOrder.getUserId())
                            .productType(productOrder.getProductType())
                            .build();

                    switch (productOrder.getProductType()){
                        case StakingProductType.FI_CURRENT_VALUE:
                            if(productOrder.getOrderType() == StakingProductOrderType.STPD_ORDER_TYPE_SUBSCRIBE_VALUE) {
                                // 申购
                                rtnCode = stakingProductCurrentStrategy.processSubscribeEvent(event);
                            }
                            break;
                        case StakingProductType.FI_TIME_VALUE:
                            rtnCode = stakingProductTimeStrategy.processSubscribeEvent(event);
                            break;
                        case StakingProductType.LOCK_POSITION_VALUE:
                            rtnCode = stakingProductLockedPositionStrategy.processSubscribeEvent(event);
                            break;
                        default:
                            break;
                    }
                    if(rtnCode != BrokerErrorCode.SUCCESS.code()){
                        log.warn("staking subscribe order process fail:code->{},order->{}", rtnCode, productOrder.toString());
                    }
                }
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    /**
     * 未处理赎回订单，补充处理
     */
    @Scheduled(cron = "0 0/5 * * * ?")
    public void processRedeemOrder() {
        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if(CollectionUtils.isEmpty(listStakingProductPermission)){
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());

        for (long orgId: orgIdList) {
            // lock
            String lockKey = String.format(StakingConstant.STAKING_SUBSCRIBE_TASK_LOCK, orgId);
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, StakingConstant.STAKING_SUBSCRIBE_TASK_LOCK_EXPIRE);
            if (!lock) {
                continue;
            }
            try {
                // 获取遗漏订单-先处理申购订单
                List<StakingProductOrder> orderList = productOrderMapper.select(StakingProductOrder.builder()
                        .orgId(orgId)
                        .orderType(1)
                        .productType(1)
                        .status(StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE)
                        .build());

                if (CollectionUtils.isEmpty(orderList)) {
                    continue;
                }

                Integer rtnCode = 0;

                for (StakingProductOrder productOrder : orderList) {
                    StakingSubscribeEvent event = StakingSubscribeEvent.builder()
                            .orgId(productOrder.getOrgId())
                            .productId(productOrder.getProductId())
                            .orderId(productOrder.getId())
                            .transferId(productOrder.getTransferId())
                            .userId(productOrder.getUserId())
                            .productType(productOrder.getProductType())
                            .build();

                    if (productOrder.getProductType() == StakingProductType.FI_CURRENT_VALUE) {
                        if (productOrder.getOrderType().compareTo(StakingProductOrderType.STPD_ORDER_TYPE_REDEEM_VALUE) == 0) {
                            StakingAsset stakingAsset = stakingAssetMapper.selectOne(StakingAsset.builder()
                                    .orgId(event.getOrgId())
                                    .productId(event.getProductId())
                                    .userId(event.getUserId())
                                    .build());
                            if (stakingAsset != null) {
                                // 赎回
                                rtnCode = stakingProductCurrentStrategy.processRedeemEvent(StakingRedeemEvent.builder()
                                        .orgId(event.getOrgId())
                                        .userId(event.getUserId())
                                        .productId(event.getProductId())
                                        .transferId(event.getTransferId())
                                        .amounts(productOrder.getPayAmount().abs())
                                        .assetId(stakingAsset.getId())
                                        .build());
                            } else {
                                log.warn("staking current redeem order data error:{}", productOrder.toString());
                            }
                        }
                    }
                    if(rtnCode != BrokerErrorCode.SUCCESS.code()){
                        log.warn("staking subscribe order process fail:code->{},order->{}", rtnCode, productOrder.toString());
                    }
                }
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    /**
     * 下线过期的产品
     */
    @Scheduled(cron = "0 0 0/8 * * ?")
    public void offlineProduct(){
        // 获取机构
        List<StakingProductPermission> listStakingProductPermission = stakingProductPermissionMapper.selectAll();
        if(CollectionUtils.isEmpty(listStakingProductPermission)){
            return;
        }
        List<Long> orgIdList = listStakingProductPermission.stream()
                .map(StakingProductPermission::getOrgId).distinct().collect(Collectors.toList());
        for(long orgId : orgIdList){
            Example productExample = new Example(StakingProduct.class);
            productExample.createCriteria().andEqualTo("orgId", orgId);
            productExample.createCriteria().andEqualTo("isShow", 1);
            List<StakingProduct> productList = productMapper.selectByExample(productExample);
            productList.forEach(product -> {
                // 检查是否存在待派息的记录
                Example rebateExample = new Example(StakingProductRebate.class);
                rebateExample.createCriteria().andEqualTo("orgId", product.getOrgId());
                rebateExample.createCriteria().andEqualTo("productId", product.getId());
                rebateExample.createCriteria().andIn("status", Arrays.asList(0,1));
                int count = stakingProductRebateMapper.selectCountByExample(rebateExample);
                if(count == 0){

                    // update isShow = 0
                    Example example = new Example(StakingProduct.class);
                    example.createCriteria().andEqualTo("id", product).andEqualTo("orgId", product.getOrgId());
                    StakingProduct updateProduct = new StakingProduct();
                    updateProduct.setIsShow(0);
                    updateProduct.setUpdatedAt(System.currentTimeMillis());
                    productMapper.updateByExampleSelective(product, example);
                }
            });
        }
    }
}
