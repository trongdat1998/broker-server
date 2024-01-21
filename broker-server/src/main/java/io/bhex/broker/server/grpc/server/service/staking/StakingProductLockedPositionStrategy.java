package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.staking.StakingProductOrderStatus;
import io.bhex.broker.grpc.staking.StakingProductRebateDetailType;
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
import java.util.List;

/**
 * 理财-券商-锁仓-产品处理策略
 *
 * @author songxd
 * @date
 */
@Slf4j
@Component
public class StakingProductLockedPositionStrategy extends AbstractSubscribeOperation {

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
        return processSubscribeRequest(stakingSchema, io.bhex.broker.grpc.staking.StakingProductType.LOCK_POSITION);
    }

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
                    processRtn = subscribeLockBalanceAndUpdateOrderStatus(productOrder, currentTime);
                } else if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE) {
                    setStatusCode(subscribeStatusKey, BrokerErrorCode.FINANCE_PURCHASE_ERROR.sCode());
                    processRtn = BrokerErrorCode.FINANCE_PURCHASE_ERROR.code();
                }
            } else {
                processRtn = BrokerErrorCode.FINANCE_PURCHASE_RECORD_NOT_EXIST.code();
            }
        } catch (Exception ex) {
            processRtn = BrokerErrorCode.FINANCE_PURCHASE_ERROR.code();
            log.error("staking product locked position processSubscribeMessage error: {},message:{}", ex, stakingSubscribeEvent.toString());
        }finally {
            RedisLockUtils.releaseLock(redisTemplate,lockKey);
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
        reCalcTimeAndLockRebate(productRebate, stakingProduct, currentTime);
    }

    /**
     * 派利息
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
                    , StakingProductType.LOCK_POSITION_VALUE
                    , stakingTransferEvent.getRebateId());
            if (CollectionUtils.isEmpty(productRebateDetailList)) {
                runStatus = false;

                // 更新该派息设置已派息完成
                productRebateMapper.updateStatus(stakingTransferEvent.getOrgId(), stakingTransferEvent.getRebateId()
                        , StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS_VALUE
                        , currentTime);
            } else {
                // 逐一转账
                EnumStakingTransfer enumStakingTransfer;
                for (StakingProductRebateDetail productRebateDetail : productRebateDetailList) {
                    // 还本，释放锁仓
                    if (productRebateDetail.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_PRINCIPAL_VALUE) {
                        enumStakingTransfer = unlockBalance(productRebateDetail);
                    }
                    // 付息
                    else {
                        enumStakingTransfer = dividendTransfer(productRebateDetail);
                    }
                    if (enumStakingTransfer == EnumStakingTransfer.SUCCESS) {
                        // update status
                        productStrategyService.updateLockPositionRebateDetailStatus(productRebateDetail);
                    }else {
                        throw new BrokerException(BrokerErrorCode.BALANCE_TRANSFER_FAILED);
                    }
                }
            }
        }
    }

    @Override
    public List<StakingSchema> listProducts(StakingSchema stakingSchema) {
        return null;
    }

    /**
     * 锁仓 并且 更新订单的状态
     *
     * @param stakingProductOrder
     * @param currentTime
     * @return
     */
    private Integer subscribeLockBalanceAndUpdateOrderStatus(StakingProductOrder stakingProductOrder, Long currentTime) {
        int rtnCode = BrokerErrorCode.SUCCESS.code();
        try {
            // lock balance
            EnumStakingTransfer enumStakingTransfer = subscribeLockBalance(stakingProductOrder);
            if (enumStakingTransfer == EnumStakingTransfer.FAIL) {
                // lock failed
                rtnCode = BrokerErrorCode.USER_ACCOUNT_TRANSFER_FILLED.code();
                productStrategyService.updateOrderFailAndProductSoldLots(stakingProductOrder, 0);
            } else if(enumStakingTransfer == EnumStakingTransfer.SUCCESS){
                // update order status = 1 and insert or update product sold_lots
                productStrategyService.updateStakingProductLockPositionOrderStatus(stakingProductOrder, 0, currentTime);
            } else{
                rtnCode = BrokerErrorCode.STAKING_PROCESSING.code();
            }
        } catch (Exception ex) {
            log.error("staking product order[lock position] exception order:{}  error:{}", stakingProductOrder.toString(), ex);
            rtnCode = BrokerErrorCode.DB_ERROR.code();
        }
        return rtnCode;
    }

    /**
     * 申购补偿，主要处理因转账超时造成的申购失败
     *
     * @param stakingProductOrder 失败订单
     */
    public void subscribeRedress(StakingProductOrder stakingProductOrder){
        try {
            // lock balance
            EnumStakingTransfer enumStakingTransfer = subscribeLockBalance(stakingProductOrder);
            if (enumStakingTransfer == EnumStakingTransfer.FAIL) {
                // 确定失败
                // stakingProductOrderMapper.updateRedressStatus(stakingProductOrder.getOrgId(), stakingProductOrder.getId(), System.currentTimeMillis());
            } else if (enumStakingTransfer == EnumStakingTransfer.SUCCESS){
                log.warn("staking lock order transfer timeout:{}", stakingProductOrder.toString());
                // 锁仓成功，但是订单失败，则补偿操作
                productStrategyService.updateStakingProductLockPositionOrderStatus(stakingProductOrder, 2, System.currentTimeMillis());
            }
        } catch (Exception ex) {
            log.error("staking subscribleRedress:{}  error:{}", stakingProductOrder.toString(), ex);
        }
    }
}
