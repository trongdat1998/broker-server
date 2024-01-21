package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.base.account.AccountType;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.domain.staking.EnumStakingTransfer;
import io.bhex.broker.server.domain.staking.StakingConstant;
import io.bhex.broker.server.domain.staking.StakingSchema;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.po.StakingSubscribeResponseDTO;
import io.bhex.broker.server.grpc.server.service.po.StakingTransferRecord;
import io.bhex.broker.server.model.staking.StakingProduct;
import io.bhex.broker.server.model.staking.StakingProductOrder;
import io.bhex.broker.server.model.staking.StakingProductRebate;
import io.bhex.broker.server.model.staking.StakingProductRebateDetail;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 理财申购操作
 *
 * @author songxd
 * @date
 */
@Slf4j
public abstract class AbstractSubscribeOperation {

    @Resource
    private ISequenceGenerator<Long> sequenceGenerator;

    @Resource
    private StakingProductStrategyService productStrategyService;

    @Autowired
    private ApplicationContext applicationContext;

    @Resource
    private StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    private StakingProductRebateMapper productRebateMapper;

    @Resource
    private StakingTransferService stakingTransferService;

    @Resource
    private StakingProductRebateDetailBatchMapper rebateDetailBatchMapper;

    @Resource
    private StakingProductRebateDetailMapper stakingProductRebateDetailMapper;

    @Autowired
    private BasicService basicService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    /**
     * 申购
     *
     * @param stakingSchema
     * @return
     */
    public abstract StakingSubscribeResponseDTO subscribe(StakingSchema stakingSchema);

    /**
     * 异步处理申购Event
     *
     * @param stakingSubscribeEvent
     * @return
     */
    public abstract Integer processSubscribeEvent(StakingSubscribeEvent stakingSubscribeEvent);

    /**
     * 计算利息
     *
     * @param productRebate
     * @param stakingProduct
     * @param currentTime
     * @return
     */
    public abstract void calcInterest(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime);

    /**
     * 派利息
     *
     * @param stakingTransferEvent
     */
    public abstract void dispatchInterest(StakingTransferEvent stakingTransferEvent);

    /**
     * 查询
     *
     * @param stakingSchema
     * @return
     */
    public abstract List<StakingSchema> listProducts(StakingSchema stakingSchema);

    /**
     * 申购请求
     *
     * @param stakingSchema
     * @param stakingProductType
     * @return
     */
    protected StakingSubscribeResponseDTO processSubscribeRequest(StakingSchema stakingSchema
            , io.bhex.broker.grpc.staking.StakingProductType stakingProductType) {

        StakingSubscribeResponseDTO responseDTO = StakingSubscribeResponseDTO.builder().transferId(0L).build();

        // 机构+理财产品+User lock
        String lockKey = String.format(StakingConstant.STAKING_SUBSCRIBE_LOCK, stakingSchema.getOrgId()
                , stakingSchema.getProductId(), stakingSchema.getUserId());

        boolean lockRtn = RedisLockUtils.tryLock(redisTemplate, lockKey, StakingConstant.STAKING_SUBSCRIBE_LOCK_EXPIRE);

        if (!lockRtn) {
            responseDTO.setCode(BrokerErrorCode.REQUEST_TOO_FAST.code());
            return responseDTO;
        }

        try {

            StakingProduct stakingProduct = productStrategyService.getStakingProduct(stakingSchema.getOrgId(), stakingSchema.getProductId());
            if (stakingProduct == null) {
                responseDTO.setCode(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code());
                return responseDTO;
            }
            // calc subscribe amount 定期、定期锁仓购买手数，所以这里计算金额
            if(stakingSchema.getAmount().compareTo(BigDecimal.ZERO) == 0) {
                stakingSchema.setAmount(BigDecimal.valueOf(stakingSchema.getLots()).multiply(stakingProduct.getPerLotAmount()));
            }
            // 活期为了兼容币多多，传过来的是申购金额，所以这里计算手数
            if(stakingSchema.getLots().compareTo(0) == 0){
                int lots = 0;
                if(stakingProduct.getPerLotAmount().compareTo(BigDecimal.ZERO) > 0){
                    lots = stakingSchema.getAmount().divide(stakingProduct.getPerLotAmount(), 0, RoundingMode.DOWN).intValue();
                }
                stakingSchema.setLots(lots);
            }

            // validation
            Integer rtnCode = productStrategyService.validation(stakingSchema, stakingProduct);
            responseDTO.setCode(rtnCode);
            if (rtnCode.compareTo(BrokerErrorCode.SUCCESS.code()) == 0) {
                long transferId = sequenceGenerator.getLong();
                responseDTO.setTransferId(transferId);

                // cache transferId
                redisTemplate.opsForValue().setIfAbsent(String.format(StakingConstant.STAKING_TRANSFER_CACHE_KEY
                        , stakingSchema.getOrgId(), stakingSchema.getUserId(), stakingSchema.getProductId(), transferId), String.valueOf(transferId)
                        , StakingConstant.STAKING_SUBSCRIBE_TRANSFER_EXPIRE_DAY, TimeUnit.DAYS);

                long currentTime = System.currentTimeMillis();

                StakingProductOrder stakingProductOrder = StakingProductOrder.builder()
                        .id(transferId)
                        .orgId(stakingSchema.getOrgId())
                        .userId(stakingSchema.getUserId())
                        .accountId(stakingSchema.getUserAccountId())
                        .productId(stakingSchema.getProductId())
                        .productType(stakingProductType.getNumber())
                        .transferId(transferId)
                        .payLots(stakingSchema.getLots())
                        .payAmount(stakingSchema.getAmount())
                        .tokenId(stakingProduct.getTokenId())
                        .takeEffectDate(stakingProduct.getInterestStartDate())
                        .canAutoRenew(0)
                        .orderType(StakingProductOrderType.STPD_ORDER_TYPE_SUBSCRIBE_VALUE)
                        .status(StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE)
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build();

                // insert staking order
                boolean insertRtn = productStrategyService.insertOrderAndUpdateProductSoldLots(stakingProductOrder);
                if (insertRtn) {
                    // 异步转账更新资产
                    CompletableFuture.runAsync(() ->
                            applicationContext.publishEvent(
                                    StakingSubscribeEvent.builder()
                                            .orgId(stakingProductOrder.getOrgId())
                                            .productId(stakingProductOrder.getProductId())
                                            .productType(stakingProductType.getNumber())
                                            .userId(stakingProductOrder.getUserId())
                                            .orderId(stakingProductOrder.getId())
                                            .transferId(stakingProductOrder.getTransferId())
                                            .build()
                            ), taskExecutor);
                }
            }
        } catch (BrokerException e) {
            responseDTO.setCode(e.code());
        } catch (Exception e) {
            responseDTO.setCode(BrokerErrorCode.FINANCE_PURCHASE_ERROR.code());
            log.error("staking subscribe exception:{},message:{}", e, stakingSchema.toString());
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }
        return responseDTO;
    }

    /**
     * 转账
     *
     * @param productOrder
     * @return
     */
    protected EnumStakingTransfer subscribeTransfer(StakingProductOrder productOrder) {

        StakingProduct stakingProduct = productStrategyService.getStakingProduct(productOrder.getOrgId(), productOrder.getProductId());
        if (stakingProduct == null) {
            return EnumStakingTransfer.FAIL;
        }

        StakingTransferRecord transferRecord = StakingTransferRecord.builder()
                .orgId(productOrder.getOrgId())
                .tokenId(productOrder.getTokenId())
                .amount(productOrder.getPayAmount())
                .transferId(productOrder.getTransferId())

                .sourceOrgId(productOrder.getOrgId())
                .sourceAccountId(productOrder.getAccountId())
                .sourceAccountType(AccountType.GENERAL_ACCOUNT)
                .sourceBusinessSubject(BusinessSubject.BUY_WEALTH_MANAGEMENT_PRODUCTS)

                .targetOrgId(productOrder.getOrgId())
                .targetAccountId(stakingProduct.getPrincipalAccountId())
                .targetAccountType(AccountType.CURRENT_DEPOSIT_BALANCE_ACCOUNT)
                .targetBusinessSubject(BusinessSubject.BUY_WEALTH_MANAGEMENT_PRODUCTS)
                .build();

        return stakingTransferService.transfer(transferRecord, 0);
    }

    /**
     * 锁仓
     *
     * @param stakingProductOrder
     * @return
     */
    protected EnumStakingTransfer subscribeLockBalance(StakingProductOrder stakingProductOrder) {
        StakingTransferRecord transferRecord = StakingTransferRecord.builder()
                .orgId(stakingProductOrder.getOrgId())
                .userId(stakingProductOrder.getUserId())
                .tokenId(stakingProductOrder.getTokenId())
                .amount(stakingProductOrder.getPayAmount())
                .transferId(stakingProductOrder.getTransferId())

                .sourceOrgId(stakingProductOrder.getOrgId())
                .sourceAccountId(stakingProductOrder.getAccountId())
                .sourceAccountType(AccountType.GENERAL_ACCOUNT)
                .sourceBusinessSubject(BusinessSubject.BUY_WEALTH_MANAGEMENT_PRODUCTS)

                .targetOrgId(stakingProductOrder.getOrgId())
                .targetAccountId(0L)
                .targetAccountType(AccountType.CURRENT_DEPOSIT_BALANCE_ACCOUNT)
                .targetBusinessSubject(BusinessSubject.BUY_WEALTH_MANAGEMENT_PRODUCTS)
                .build();

        return stakingTransferService.lockBalance(transferRecord, 0);
    }

    /**
     * 设置申购、赎回 状态码
     */
    protected void setStatusCode(String key, String statusCode) {
        redisTemplate.opsForValue().set(key, statusCode, StakingConstant.STAKING_STATUS_CACHE_EXPIRE_MIN, TimeUnit.MINUTES);
    }

    /**
     * 转账 and Update
     *
     * @param stakingProductOrder
     * @param currentTime
     * @return
     */
    protected Integer subscribeTransferAndUpdateOrderStatus(StakingProductOrder stakingProductOrder, Long currentTime) {
        int rtnCode = BrokerErrorCode.SUCCESS.code();
        try {
            // transfer
            EnumStakingTransfer enumStakingTransfer = subscribeTransfer(stakingProductOrder);
            if(enumStakingTransfer == EnumStakingTransfer.SUCCESS){
                // update order status = 1 and insert or update asset
                productStrategyService.updateOrderStatusAndInsertAsset(stakingProductOrder, 0, currentTime);
            } else if(enumStakingTransfer == EnumStakingTransfer.FAIL){
                rtnCode = BrokerErrorCode.USER_ACCOUNT_TRANSFER_FILLED.code();
                productStrategyService.updateOrderFailAndProductSoldLots(stakingProductOrder, 0);
            } else{
                // 处理中
                rtnCode = BrokerErrorCode.STAKING_PROCESSING.code();
            }
        } catch (Exception ex) {
            log.error("staking subscribeTransferAndUpdateOrderStatus exception:{}  error:{}", stakingProductOrder.toString(), ex);
            rtnCode = BrokerErrorCode.DB_ERROR.code();
        }
        return rtnCode;
    }

    /**
     * 计算利息
     */
    protected void calcTimeAndLockRebate(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime) {

        List<StakingProductOrder> orderList;
        boolean calcOrder = true;
        boolean firstGetOrders = true;

        // 1. 计息天数
        Long spanDays = getStakingProductOrderSpanDays(productRebate, stakingProduct);

        if (spanDays.compareTo(0L) <= 0) {
            log.warn("staking product rebate date set error:{},spanDays:{}", productRebate.toString(),spanDays);
            productRebateMapper.updateStatus(productRebate.getOrgId(), productRebate.getId()
                    , StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE, System.currentTimeMillis());
            return;
        }

        // 2. 是否最后一次派息
        boolean isFinally = checkStakingProductRebateIsFinally(productRebate);
        StakingProductRebate principalRebate = null;
        if (isFinally) {
            principalRebate = productRebateMapper.getPrincipalRebate(productRebate.getOrgId(), productRebate.getProductId());
            if (principalRebate == null) {
                // 兼容老数据，如果没有设置还本配置
                principalRebate = productRebate;
            }
        }

        // 根据理财项目的派息设置，把理财产品的申购记录拉出来，进行逐一计算，然后保存计算结果，等待后台审核确认转账
        while (calcOrder) {

            // 3. 循环获取申购记录
            orderList = stakingProductOrderMapper.listProductRebateOrder(productRebate.getOrgId(), productRebate.getProductId()
                    , productRebate.getId(), StakingConstant.BATCH_LIST_SIZE);
            if ( CollectionUtils.isEmpty(orderList)) {
                // calc end
                calcOrder = false;

                if(firstGetOrders){
                    // 如果没有订单数据则状态改为无效
                    productRebateMapper.updateInvalidStatus(productRebate.getOrgId(), productRebate.getId()
                            , System.currentTimeMillis());

                    if(isFinally && principalRebate != null){
                        productRebateMapper.updateInvalidStatus(principalRebate.getOrgId(), principalRebate.getId()
                                , System.currentTimeMillis());
                    }
                } else {
                    try {
                        // 5. 更新本次派息设置状态及派息金额
                        updateStakingProductRebateStatusAndAmountOrRate(productRebate, stakingProduct, spanDays, isFinally, principalRebate, currentTime);
                    } catch (Exception ex) {
                        log.error("[DBError] staking product rebate update status error：{} productRebate:{}", ex, productRebate.toString());
                        return;
                    }
                }

                continue;
            }
            firstGetOrders = false;
            // 4. db 计算利息并保存派息记录
            insertProductOrderRebateDetail(productRebate, orderList, stakingProduct, spanDays, isFinally, principalRebate);
        }
    }

    /**
     * 计算利息
     */
    protected void reCalcTimeAndLockRebate(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime) {

        List<StakingProductOrder> orderList;
        boolean calcOrder = true;

        // 1. 计息天数
        Long spanDays = getStakingProductOrderSpanDays(productRebate, stakingProduct);

        if (spanDays.compareTo(0L) <= 0) {
            log.warn("staking product rebate date set error:{}", productRebate.toString());
            productRebateMapper.updateStatus(productRebate.getOrgId(), productRebate.getId()
                    , StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE, System.currentTimeMillis());
            return;
        }

        // 根据理财项目的派息设置，把理财产品的申购记录拉出来，进行逐一计算，然后保存计算结果，等待后台审核确认转账
        while (calcOrder) {

            // 3. 循环获取申购记录
            orderList = stakingProductOrderMapper.listReCalcProductOrders(productRebate.getOrgId(), productRebate.getProductId()
                    , productRebate.getId(), StakingConstant.BATCH_LIST_SIZE);
            if (CollectionUtils.isEmpty(orderList)) {
                // calc end
                calcOrder = false;

                try {
                    // 5. 更新本次派息设置状态及派息金额
                    updateStakingProductRebateStatusAndAmountOrRate(productRebate, stakingProduct, spanDays,false, null, currentTime);
                } catch (Exception ex) {
                    log.error("[DBError] staking product rebate update status error：{} productRebate:{}", ex, productRebate.toString());
                    return;
                }

                continue;
            }

            // 4. db 计算利息并保存派息记录
            insertProductOrderRebateDetail(productRebate, orderList, stakingProduct, spanDays,false, null);
        }
    }

    /**
     * 计算本金
     */
    protected void calcPrincipalAmount(StakingProductRebate productRebate, StakingProduct stakingProduct) {

        List<StakingProductOrder> orderList;
        boolean calcOrder = true;

        // 根据理财项目的派息设置，把理财产品的申购记录拉出来，进行逐一计算，然后保存计算结果，等待后台审核确认转账
        while (calcOrder) {

            // 1. 循环获取申购记录
            orderList = stakingProductOrderMapper.listProductRebateOrder(productRebate.getOrgId(), productRebate.getProductId()
                    , productRebate.getId(), StakingConstant.BATCH_LIST_SIZE);
            if (CollectionUtils.isEmpty(orderList)) {
                // calc end
                calcOrder = false;

                try {
                    // 3. 更新本次还本的总金额
                    updatePrincipalRebateStatusAndAmount(productRebate);
                } catch (Exception ex) {
                    log.error("[DBError] staking calcPrincipalAmount error：{} productRebate:{}", ex, productRebate.toString());
                    return;
                }

                continue;
            }

            // 2. db 计算还本记录
            insertPrincipalRebateDetail(productRebate, orderList, stakingProduct);
        }
    }

    /**
     * 保存计算后的派息记录同时更新派息设置状态
     *
     * @param productRebate   派息配置
     * @param orderList       申购订单列表
     * @param stakingProduct  申购产品
     * @param spanDays        计息天数
     * @param isFinally       是否最后一次计息
     * @param principalRebate 还本配置
     */
    private void insertProductOrderRebateDetail(StakingProductRebate productRebate, List<StakingProductOrder> orderList
            , StakingProduct stakingProduct, Long spanDays, Boolean isFinally, StakingProductRebate principalRebate) {

        BigDecimal interestAmount;

        if ( CollectionUtils.isEmpty(orderList)) {
            return;
        }

        Long currentTime = System.currentTimeMillis();
        List<StakingProductRebateDetail> listRebateDetail = new ArrayList<>();

        for (StakingProductOrder productOrder : orderList) {

            // calc interest amount
            interestAmount = calcInterestByRebate(productRebate, stakingProduct
                    , productOrder.getPayAmount(), new BigDecimal(spanDays));

            // 判断利息是否为零,先不判断了，因为如果理财产品设置的最低申购不合理，计算的利息被舍弃，这里是有可能为零的
            /*if (interestAmount.compareTo(BigDecimal.ZERO) == 0) {
                continue;
            }*/

            // 派息记录
            listRebateDetail.add(constructionRebateDetail(productOrder, productRebate, interestAmount, stakingProduct.getDividendAccountId()
                    , StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_INTEREST_VALUE, currentTime));

            // 如果是最后一次派息，则还本
            if (isFinally && principalRebate != null) {
                StakingProductRebateDetail rebateDetail = constructionRebateDetail(productOrder, productRebate, productOrder.getPayAmount()
                        , stakingProduct.getPrincipalAccountId(), StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_PRINCIPAL_VALUE, currentTime);
                rebateDetail.setProductRebateId(principalRebate.getId());
                rebateDetail.setTokenId(productOrder.getTokenId());
                if (principalRebate.getType() == StakingProductRebateType.STPD_REBATE_TYPE_PRINCIPAL_VALUE
                        && principalRebate.getRebateRate().compareTo(BigDecimal.ZERO) > 0
                        && principalRebate.getRebateRate().compareTo(BigDecimal.valueOf(1L)) != 0) {
                    // 如果还本金配置中配置的比率不等于1，则重新计算本金金额(不保本)
                    rebateDetail.setRebateAmount(principalRebate.getRebateRate().multiply(rebateDetail.getRebateAmount()));
                }
                listRebateDetail.add(rebateDetail);
            }
        }
        try {
            if (! CollectionUtils.isEmpty(listRebateDetail)) {
                // 保存派息计算结果
                rebateDetailBatchMapper.insertList(listRebateDetail);
            }
        } catch (Exception ex) {
            log.error("staking (insertProductOrderRebateDetail)：ex->{},params->{}", ex
                    , orderList.stream().map(StakingProductOrder::getId).collect(Collectors.toList()).toString());
        } finally {
            listRebateDetail.clear();
        }
    }

    /**
     * 保存还本记录
     *
     * @param productRebate   派息配置
     * @param orderList       申购订单列表
     * @param stakingProduct  申购产品
     */
    private void insertPrincipalRebateDetail(StakingProductRebate productRebate, List<StakingProductOrder> orderList
            , StakingProduct stakingProduct) {

        if (CollectionUtils.isEmpty(orderList)) {
            return;
        }

        Long currentTime = System.currentTimeMillis();
        List<StakingProductRebateDetail> listRebateDetail = new ArrayList<>();

        for (StakingProductOrder productOrder : orderList) {
            StakingProductRebateDetail rebateDetail = constructionRebateDetail(productOrder, productRebate
                    , productRebate.getRebateRate().multiply(productOrder.getPayAmount())
                    , stakingProduct.getPrincipalAccountId()
                    , StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_PRINCIPAL_VALUE, currentTime);
            listRebateDetail.add(rebateDetail);
        }
        try {
            if (! CollectionUtils.isEmpty(listRebateDetail)) {
                rebateDetailBatchMapper.insertList(listRebateDetail);
            }
        } catch (Exception ex) {
            log.error("staking (insertPrincipalRebateDetail)：ex->{},params-orders->{}", ex
                    , orderList.stream().map(StakingProductOrder::getId).collect(Collectors.toList()).toString());
        } finally {
            listRebateDetail.clear();
        }
    }

    /**
     * 计算该理财产品本次派息的计息天数
     *
     * @param productRebate
     * @param stakingProduct
     * @return
     */
    private Long getStakingProductOrderSpanDays(StakingProductRebate productRebate, StakingProduct stakingProduct) {

        long spanDays;

        // 上一次派息记录
        StakingProductRebate preProductRebate = productRebateMapper.getPreStakingProductRebate(productRebate.getOrgId(), productRebate.getProductId()
                , productRebate.getId(), productRebate.getProductType(), productRebate.getRebateDate());
        // 计息天数，注意：第一次派息和第二次派息计息天数的计算基础是不一样的
        if (preProductRebate == null) {
            spanDays = TimeUnit.MILLISECONDS.toDays(productRebate.getRebateDate() - stakingProduct.getInterestStartDate());
        } else {
            spanDays = TimeUnit.MILLISECONDS.toDays(productRebate.getRebateDate() - preProductRebate.getRebateDate());
        }
        return spanDays;
    }

    /**
     * 判断当前是否最后一次派息
     *
     * @param productRebate
     * @return
     */
    protected Boolean checkStakingProductRebateIsFinally(StakingProductRebate productRebate) {
        // 1.检查当前派息记录是否是该产品最后一次派息
        boolean lastInterest = false;
        StakingProductRebate productRebateCheck = productRebateMapper.checkIsLastInterest(productRebate.getOrgId(), productRebate.getProductId()
                , productRebate.getId(), productRebate.getProductType());
        // 如果找不到其他未派息的记录，则该派息设置为最后一次派息
        if (productRebateCheck == null) {
            lastInterest = true;
        }
        return lastInterest;
    }

    /**
     * 理财申购：派息转账
     *
     * @param productRebateDetail
     * @return
     */
    protected EnumStakingTransfer dividendTransfer(StakingProductRebateDetail productRebateDetail) {

        if (productRebateDetail.getRebateAmount().compareTo(BigDecimal.ZERO) <= 0) {
            return EnumStakingTransfer.SUCCESS;
        }

        return stakingTransferService.transfer(StakingTransferRecord.builder()
                .orgId(productRebateDetail.getOrgId())
                .tokenId(productRebateDetail.getTokenId())
                .amount(productRebateDetail.getRebateAmount())
                .transferId(productRebateDetail.getTransferId())

                .sourceOrgId(productRebateDetail.getOrgId())
                .sourceAccountId(productRebateDetail.getOriginAccountId())
                .sourceAccountType(
                        productRebateDetail.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_INTEREST_VALUE ? AccountType.PAY_INTEREST_ACCOUNT : AccountType.PAY_PRINCIPAL_ACCOUNT
                )
                .sourceBusinessSubject(
                        productRebateDetail.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_INTEREST_VALUE ? BusinessSubject.INTEREST_WEALTH_MANAGEMENT_PRODUCTS : BusinessSubject.PRINCIPAL_WEALTH_MANAGEMENT_PRODUCTS)

                .targetOrgId(productRebateDetail.getOrgId())
                .targetAccountId(productRebateDetail.getUserAccountId())
                .targetAccountType(AccountType.GENERAL_ACCOUNT)
                .targetBusinessSubject(
                        productRebateDetail.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_INTEREST_VALUE ? BusinessSubject.INTEREST_WEALTH_MANAGEMENT_PRODUCTS : BusinessSubject.PRINCIPAL_WEALTH_MANAGEMENT_PRODUCTS
                )
                .build(), 0);
    }

    /**
     * 理财：锁仓
     *
     * @param productRebateDetail
     * @return
     */
    protected EnumStakingTransfer unlockBalance(StakingProductRebateDetail productRebateDetail) {

        return stakingTransferService.unLockBalance(StakingTransferRecord.builder()
                .orgId(productRebateDetail.getOrgId())
                .userId(productRebateDetail.getUserId())
                .tokenId(productRebateDetail.getTokenId())
                .amount(productRebateDetail.getRebateAmount())
                .transferId(productRebateDetail.getTransferId())

                .sourceOrgId(productRebateDetail.getOrgId())
                .sourceAccountId(productRebateDetail.getUserAccountId())
                .sourceAccountType(AccountType.GENERAL_ACCOUNT)
                .sourceBusinessSubject(BusinessSubject.PRINCIPAL_WEALTH_MANAGEMENT_PRODUCTS)

                .build(), 0);
    }

    /**
     * 检测TransferId是否合法
     *
     * @param orgId
     * @param userId
     * @param productId
     * @param transferId
     * @return
     */
    protected Boolean checkTransferIdIsLegal(Long orgId, Long userId, Long productId, Long transferId) {
        String strTransferId = redisTemplate.opsForValue().get(String.format(StakingConstant.STAKING_TRANSFER_CACHE_KEY
                , orgId, userId, productId, transferId));
        return StringUtils.isNoneEmpty(strTransferId) && String.valueOf(transferId).equals(strTransferId);
    }

    /**
     * 更新派息设置的状态及派息金额
     *
     * @param productRebate   本次派息设置
     * @param stakingProduct  理财产品
     * @param spanDays        计息天数
     * @param isFinally       本次是否是最后一次派息
     * @param principalRebate 本金配置
     * @param currentTime     时间戳
     */
    protected void updateStakingProductRebateStatusAndAmountOrRate(StakingProductRebate productRebate
            , StakingProduct stakingProduct, Long spanDays, Boolean isFinally, StakingProductRebate principalRebate, Long currentTime) {

        BigDecimal apr = null;
        BigDecimal totalInterest;

        if (productRebate.getRebateCalcWay() == StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_AMOUNT_VALUE) {
            totalInterest = null;
            // 计算年化利率
            apr = calcRateByAmount(productRebate,stakingProduct,spanDays);
        } else {
            // 幂等：所以这里的本次派息总金额读取数据库
            totalInterest = stakingProductRebateDetailMapper.sumRebateAmount(productRebate.getOrgId(), productRebate.getProductId(), productRebate.getId());
        }

        // 更新计算状态同时更细年利率或本次派息金额
        productRebateMapper.updateStatusAndAmountOrRate(
                StakingProductRebate.builder()
                        .id(productRebate.getId())
                        .orgId(productRebate.getOrgId())
                        .status(StakingProductRebateStatus.STPD_REBATE_STATUS_CALCED_VALUE)
                        .updatedAt(currentTime)
                        .rebateRate(apr)
                        .rebateAmount(totalInterest)
                        .build()
        );

        // 如果是最后一次派息, 单独设置了还本金配置
        if (isFinally && principalRebate != null && !principalRebate.getId().equals(productRebate.getId())) {
            // 更新派息设置中的本金派息设置记录 还本总金额、利率 1
            BigDecimal principalAmount = stakingProductRebateDetailMapper.sumPrincipalRebateAmount(productRebate.getOrgId()
                    , productRebate.getProductId(), principalRebate.getId());
            productRebateMapper.updatePrincipalRebate(productRebate.getOrgId()
                    , productRebate.getProductId(), principalRebate.getId(), principalAmount == null ? BigDecimal.ZERO : principalAmount, currentTime);
        }
    }

    /**
     * 根据利息计算利率
     * @param productRebate
     * @param stakingProduct
     * @param spanDays
     * @return
     */
    private BigDecimal calcRateByAmount(StakingProductRebate productRebate, StakingProduct stakingProduct, Long spanDays) {
        if(stakingProduct.getSoldLots().equals(0) || spanDays.equals(0L)){
            return BigDecimal.ZERO;
        }
        BigDecimal apr = null;
        if(productRebate.getTokenId().equals(stakingProduct.getTokenId())) {
            apr = productRebate.getRebateAmount().divide(
                    BigDecimal.valueOf(stakingProduct.getSoldLots()).multiply(stakingProduct.getPerLotAmount())
                    , StakingConstant.DEFAULT_SCALE, RoundingMode.DOWN)
                    .multiply(StakingConstant.DAY_OF_YEAR)
                    .divide(new BigDecimal(spanDays), StakingConstant.RATE_SCALE, RoundingMode.DOWN);
        } else{
            // 获取汇率
            Rate fxRate = basicService.getV3Rate(productRebate.getOrgId(), productRebate.getTokenId());
            if(fxRate == null) {
                apr = BigDecimal.ZERO;
            } else {
                if(fxRate.getRatesMap().containsKey(stakingProduct.getTokenId())) {
                    apr = productRebate.getRebateAmount().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get(stakingProduct.getTokenId())))
                            .divide(BigDecimal.valueOf(stakingProduct.getSoldLots()).multiply(stakingProduct.getPerLotAmount())
                                    , StakingConstant.DEFAULT_SCALE, RoundingMode.DOWN)
                            .multiply(StakingConstant.DAY_OF_YEAR)
                            .divide(new BigDecimal(spanDays), StakingConstant.RATE_SCALE, RoundingMode.DOWN);
                } else{
                    apr = BigDecimal.ZERO;
                }
            }
        }
        return apr;
    }

    /**
     * 更新还本设置的状态及还本金额
     *
     * @param productRebate   还本设置
     */
    protected void updatePrincipalRebateStatusAndAmount(StakingProductRebate productRebate) {
        // 更新还本
        BigDecimal principalAmount = stakingProductRebateDetailMapper.sumPrincipalRebateAmount(productRebate.getOrgId()
                , productRebate.getProductId(), productRebate.getId());
        if (principalAmount != null) {
            productRebateMapper.updatePrincipalRebate(productRebate.getOrgId(), productRebate.getProductId()
                    , productRebate.getId(), principalAmount, System.currentTimeMillis());
        }
    }

    /**
     * 计算利息
     *
     * @param productRebate  利率设置
     * @param stakingProduct 产品信息
     * @param amount         申购金额
     * @param days           计息天数
     * @return
     */
    protected BigDecimal calcInterestByRebate(StakingProductRebate productRebate
            , StakingProduct stakingProduct, BigDecimal amount, BigDecimal days) {

        BigDecimal calcInterestAmount = BigDecimal.ZERO;

        if (productRebate.getRebateCalcWay() == StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_RATE_VALUE
                && productRebate.getRebateRate().compareTo(BigDecimal.ZERO) > 0) {
            // 利率计算利息
            calcInterestAmount = amount.multiply(productRebate.getRebateRate())
                    .multiply(days)
                    .divide(StakingConstant.DAY_OF_YEAR, StakingConstant.INTEREST_SCALE, RoundingMode.DOWN);
        } else if (productRebate.getRebateCalcWay() == StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_AMOUNT_VALUE
                && productRebate.getRebateAmount().compareTo(BigDecimal.ZERO) > 0) {
            // 派息金额分摊
            calcInterestAmount = productRebate.getRebateAmount().multiply(amount)
                    .divide(BigDecimal.valueOf(stakingProduct.getSoldLots()).multiply(stakingProduct.getPerLotAmount())
                            , StakingConstant.INTEREST_SCALE, RoundingMode.DOWN);
        } else {
            log.warn("staking product calc interest setting failed :{}", productRebate.toString());
        }
        return calcInterestAmount;
    }

    /**
     * 构造StakingProductRebateDetail
     *
     * @param productOrder
     * @param productRebate
     * @param originAccountId
     * @param rebateType
     * @param currentTime
     * @return
     */
    private StakingProductRebateDetail constructionRebateDetail(StakingProductOrder productOrder, StakingProductRebate productRebate
            , BigDecimal rebateAmount, Long originAccountId, Integer rebateType, Long currentTime) {

        return StakingProductRebateDetail.builder()
                .orgId(productOrder.getOrgId())
                .userId(productOrder.getUserId())
                .productId(productOrder.getProductId())
                .productType(productOrder.getProductType())
                .orderId(productOrder.getId())
                .productRebateId(productRebate.getId())
                .tokenId(productRebate.getTokenId())
                .userAccountId(productOrder.getAccountId())
                .originAccountId(originAccountId)
                .transferId(sequenceGenerator.getLong())
                .rebateType(rebateType)
                .rebateAmount(rebateAmount)
                .status(StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_WAITING_VALUE)
                .createdAt(currentTime)
                .updatedAt(currentTime)
                .build();
    }
}
