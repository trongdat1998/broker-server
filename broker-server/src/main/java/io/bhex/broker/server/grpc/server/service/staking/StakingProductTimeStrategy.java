package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.staking.StakingProductOrderStatus;
import io.bhex.broker.grpc.staking.StakingProductRebateStatus;
import io.bhex.broker.grpc.staking.StakingProductType;
import io.bhex.broker.server.domain.staking.EnumStakingTransfer;
import io.bhex.broker.server.domain.staking.StakingConstant;
import io.bhex.broker.server.domain.staking.StakingSchema;
import io.bhex.broker.server.grpc.server.service.po.StakingSubscribeResponseDTO;
import io.bhex.broker.server.model.staking.StakingProduct;
import io.bhex.broker.server.model.staking.StakingProductOrder;
import io.bhex.broker.server.model.staking.StakingProductRebate;
import io.bhex.broker.server.model.staking.StakingProductRebateDetail;
import io.bhex.broker.server.primary.mapper.StakingProductOrderMapper;
import io.bhex.broker.server.primary.mapper.StakingProductRebateDetailMapper;
import io.bhex.broker.server.primary.mapper.StakingProductRebateMapper;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;

/**
 * 理财-券商-定期-产品处理策略
 *
 * @author songxd
 * @date
 */
@Component
@Slf4j
public class StakingProductTimeStrategy extends AbstractSubscribeOperation {

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    private StakingProductRebateMapper productRebateMapper;

    @Resource
    private StakingProductRebateDetailMapper productRebateDetailMapper;

    @Resource
    private StakingProductStrategyService productStrategyService;

    /**
     * 申购
     *
     * @param stakingSchema
     * @return
     */
    @Override
    public StakingSubscribeResponseDTO subscribe(StakingSchema stakingSchema) {
        return processSubscribeRequest(stakingSchema, io.bhex.broker.grpc.staking.StakingProductType.FI_TIME);
    }

    /**
     * 处理申购：转账 + 处理资产信息
     *
     * @param stakingSubscribeEvent
     * @return
     */
    @Override
    public Integer processSubscribeEvent(StakingSubscribeEvent stakingSubscribeEvent) {
        Integer processRtn = BrokerErrorCode.SUCCESS.code();

        String subscribeStatusKey = String.format(StakingConstant.SUBSCRIBE_STATUS_CODE_PREFIX
                , stakingSubscribeEvent.getOrgId()
                , stakingSubscribeEvent.getUserId()
                , stakingSubscribeEvent.getProductId()
                , stakingSubscribeEvent.getTransferId());

        String lockKey = String.format(StakingConstant.STAKING_SUBSCRIBE_EVENT_LOCK_KEY
                , stakingSubscribeEvent.getOrgId()
                , stakingSubscribeEvent.getUserId()
                , stakingSubscribeEvent.getProductId()
                , stakingSubscribeEvent.getTransferId());

        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, StakingConstant.STAKING_REDEEM_LOCK_EXPIRE_SECOND * 30);
        if (!lock) {
            return BrokerErrorCode.REQUEST_TOO_FAST.code();
        }
        try {
            Long currentTime = System.currentTimeMillis();
            StakingProductOrder productOrder = stakingProductOrderMapper.getByTransferId(stakingSubscribeEvent.getOrgId()
                    , stakingSubscribeEvent.getUserId()
                    , stakingSubscribeEvent.getProductId()
                    , stakingSubscribeEvent.getTransferId());
            if (productOrder != null) {
                // no transfer
                if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE) {
                    // 转账并且更新状态
                    processRtn = subscribeTransferAndUpdateOrderStatus(productOrder, currentTime);
                } else if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE) {
                    processRtn = BrokerErrorCode.FINANCE_PURCHASE_ERROR.code();
                }
            } else {
                processRtn = BrokerErrorCode.FINANCE_PURCHASE_RECORD_NOT_EXIST.code();
            }
        } catch (Exception ex) {
            processRtn = BrokerErrorCode.FINANCE_PURCHASE_ERROR.code();
            log.error("staking product time processSubscribeEvent error: {},message:{}", ex, stakingSubscribeEvent.toString());
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }
        setStatusCode(subscribeStatusKey, processRtn.toString());
        return processRtn;
    }

    /**
     * 计算利息
     *
     * @param productRebate
     * @param stakingProduct
     * @param currentTime
     */
    @Override
    public void calcInterest(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime) {
        calcTimeAndLockRebate(productRebate, stakingProduct, currentTime);
    }

    /**
     * 重新计算利息
     * @param productRebate
     * @param stakingProduct
     * @param currentTime
     */
    public void reCalcInterest(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime) {
        if(productRebate.getType().equals(0)){
            // 利息计算
            reCalcTimeAndLockRebate(productRebate, stakingProduct, currentTime);
        } else if(productRebate.getType().equals(1)){
            // 判断目前是否有有效的还本记录，如果没有则进行重新计算
            List<StakingProductRebateDetail> listPrincipal = productRebateDetailMapper.getPrincipalList(productRebate.getOrgId(),stakingProduct.getId(),stakingProduct.getType());
            if(CollectionUtils.isEmpty(listPrincipal)) {
                calcPrincipalAmount(productRebate, stakingProduct);
            }
        }
    }

    /**
     * 派息
     *
     * @param stakingTransferEvent
     */
    @Override
    public void dispatchInterest(StakingTransferEvent stakingTransferEvent) {
        boolean runStatus = true;
        Long currentTime = System.currentTimeMillis();
        List<StakingProductRebateDetail> productRebateDetailList;

        while (runStatus) {
            // select
            productRebateDetailList = productRebateDetailMapper.getWatingTransferList(stakingTransferEvent.getOrgId()
                    , stakingTransferEvent.getProductId()
                    , StakingProductType.FI_TIME_VALUE
                    , stakingTransferEvent.getRebateId());
            if (CollectionUtils.isEmpty(productRebateDetailList)) {
                runStatus = false;

                // 更新该派息设置已派息完成
                productRebateMapper.updateStatus(stakingTransferEvent.getOrgId(), stakingTransferEvent.getRebateId()
                        , StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS_VALUE
                        , currentTime);
            } else {
                // 逐一转账
                for (StakingProductRebateDetail productRebateDetail : productRebateDetailList) {
                    EnumStakingTransfer transferResult = dividendTransfer(productRebateDetail);
                    if (transferResult == EnumStakingTransfer.SUCCESS) {
                        // update status
                        productStrategyService.updateTimeRebateDetailStatus(productRebateDetail);
                    } else{
                        throw new BrokerException(BrokerErrorCode.BALANCE_TRANSFER_FAILED);
                    }
                }
            }
        }
    }

    /**
     * 失败订单补偿
     *
     * @param stakingProductOrder 失败订单
     * @return
     */
    public void subscribeRedress(StakingProductOrder stakingProductOrder) {
        try {
            // transfer
            EnumStakingTransfer enumStakingTransfer = subscribeTransfer(stakingProductOrder);
            if (enumStakingTransfer == EnumStakingTransfer.FAIL) {
                //stakingProductOrderMapper.updateRedressStatus(stakingProductOrder.getOrgId(), stakingProductOrder.getId(), System.currentTimeMillis());
            } else if(enumStakingTransfer == EnumStakingTransfer.SUCCESS){
                // 4. update order status = 1 and insert or update asset
                log.warn("staking time order transfer timeout:{}", stakingProductOrder.toString());
                productStrategyService.updateOrderStatusAndInsertAsset(stakingProductOrder, 2, System.currentTimeMillis());
            }
        } catch (Exception ex) {
            log.error("staking subscribleRedress exception:{}  error:{}", stakingProductOrder.toString(), ex);
        }
    }

    @Override
    public List<StakingSchema> listProducts(StakingSchema stakingSchema) {
        return null;
    }
}
