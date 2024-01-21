package io.bhex.broker.server.grpc.server.service.staking;

import com.google.protobuf.TextFormat;
import io.bhex.base.account.AccountType;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.domain.staking.*;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.po.StakingSubscribeResponseDTO;
import io.bhex.broker.server.grpc.server.service.po.StakingTransferRecord;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.BrokerSlackUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 理财-券商-活期-产品处理策略
 *
 * @author songxd
 * @date
 */
@Component
@Slf4j
public class StakingProductCurrentStrategy extends AbstractSubscribeOperation {

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    private StakingProductRebateMapper productRebateMapper;

    @Resource
    private StakingProductRebateDetailMapper productRebateDetailMapper;

    @Resource
    private StakingAssetMapper stakingAssetMapper;

    @Resource
    private StakingAssetSnapshotMapper assetSnapshotMapper;

    @Resource
    private StakingAssetSnapshotBatchMapper stakingAssetSnapshotBatchMapper;

    @Resource
    private ISequenceGenerator<Long> sequenceGenerator;

    @Resource
    private StakingProductStrategyService productStrategyService;

    @Resource
    private AccountService accountService;

    @Resource
    private StakingTransferService stakingTransferService;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private BrokerSlackUtil brokerSlackUtil;

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * 转账失败计数器
     */
    private final static Integer TRANSFER_FAILED_COUNTER = 5;

    /**
     * 活期申购
     *
     * @param stakingSchema
     * @return
     */
    @Override
    public StakingSubscribeResponseDTO subscribe(StakingSchema stakingSchema) {
        return processSubscribeRequest(stakingSchema, io.bhex.broker.grpc.staking.StakingProductType.FI_CURRENT);
    }

    /**
     * 处理申购：转账 + 更新资产信息
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
                if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING.getNumber()) {
                    // 转账并且更新状态
                    processRtn = subscribeTransferAndUpdateOrderStatus(productOrder, currentTime);
                } else if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED.getNumber()) {
                    processRtn = BrokerErrorCode.FINANCE_PURCHASE_ERROR.code();
                }
            } else {
                processRtn = BrokerErrorCode.FINANCE_PURCHASE_RECORD_NOT_EXIST.code();
            }
        } catch (Exception ex) {
            processRtn = BrokerErrorCode.FINANCE_PURCHASE_ERROR.code();
            log.error("staking product current processSubscribeMessage error: {},message:{}", ex, stakingSubscribeEvent.toString());
        }
        finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }
        setStatusCode(subscribeStatusKey, processRtn.toString());
        return processRtn;
    }


    /**
     * 赎回请求处理
     *
     * @param header    header
     * @param productId 产品ID
     * @param assetId   资产ID
     * @param amount    赎回金额
     * @return
     */
    public Integer processRedeemRequest(Header header, Long productId, Long assetId, BigDecimal amount, Long transferId) {
        Integer rtnCode;

        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return BrokerErrorCode.PARAM_ERROR.code();
        }

        boolean lock;

        String redeemLock = String.format(StakingConstant.STAKING_REDEEM_LOCK_KEY, header.getOrgId(), header.getUserId(), productId, assetId);
        lock = RedisLockUtils.tryLock(redisTemplate, redeemLock, StakingConstant.STAKING_REDEEM_LOCK_EXPIRE_SECOND);
        if (!lock) {
            return BrokerErrorCode.REQUEST_TOO_FAST.code();
        } else {
            try {
                long currentTime = System.currentTimeMillis();
                // 获取商品信息: 还本账户
                StakingProduct stakingProduct = productStrategyService.getStakingProduct(header.getOrgId(), productId);
                if (stakingProduct == null) {
                    // 商品信息不存在
                    return BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code();
                }
                // 项目是否已经结束
                if (stakingProduct.getSubscribeEndDate() < currentTime) {
                    // 表示项目已结束，停止赎回，自动还本[后台操作还本]
                    return BrokerErrorCode.FINANCE_PRODUCT_STOP_REDEEM.code();
                }
                if (amount.subtract(stakingProduct.getPerLotAmount()).remainder(stakingProduct.getPerLotAmount()).compareTo(BigDecimal.ZERO) != 0) {
                    return BrokerErrorCode.FINANCE_ILLEGAL_REDEEM_AMOUNT.code();
                }
                // validation
                rtnCode = redeemValidation(assetId, header.getOrgId(), amount);

                if (rtnCode == BrokerErrorCode.SUCCESS.code()) {
                    // get user accountId
                    Long accountId = accountService.getAccountId(header.getOrgId(), header.getUserId());
                    // add redeem order

                    int lots = 0;
                    if (stakingProduct.getPerLotAmount().compareTo(BigDecimal.ZERO) > 0) {
                        lots = amount.divide(stakingProduct.getPerLotAmount(), 0, RoundingMode.DOWN).intValue();
                    }
                    StakingProductOrder stakingProductOrder = StakingProductOrder.builder()
                            .id(transferId)
                            .orgId(header.getOrgId())
                            .userId(header.getUserId())
                            .accountId(accountId)
                            .productId(productId)
                            .productType(StakingProductType.FI_CURRENT_VALUE)
                            .tokenId(stakingProduct.getTokenId())
                            .transferId(transferId)
                            .payLots(lots)
                            .payAmount(BigDecimal.ZERO.subtract(amount))
                            .status(StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE)
                            .orderType(StakingProductOrderType.STPD_ORDER_TYPE_REDEEM_VALUE)
                            .createdAt(currentTime)
                            .updatedAt(currentTime)
                            .build();
                    // insert staking product redeem order
                    try {
                        // 保存赎回订单同时扣减资产
                        boolean insertRnt = productStrategyService.insertRedeemOrderAndReduceAsset(stakingProductOrder);
                        if (insertRnt) {
                            // cache transferId
                            redisTemplate.opsForValue().setIfAbsent(String.format(StakingConstant.STAKING_TRANSFER_CACHE_KEY
                                    , header.getOrgId(), header.getUserId(), productId, transferId), String.valueOf(transferId)
                                    , StakingConstant.STAKING_SUBSCRIBE_TRANSFER_EXPIRE_DAY, TimeUnit.DAYS);

                            // 异步转账更新资产
                            CompletableFuture.runAsync(() ->
                                    applicationContext.publishEvent(
                                            StakingRedeemEvent.builder()
                                                    .orgId(stakingProductOrder.getOrgId())
                                                    .productId(stakingProductOrder.getProductId())
                                                    .userId(stakingProductOrder.getUserId())
                                                    .assetId(assetId)
                                                    .amounts(amount)
                                                    .transferId(transferId)
                                                    .build()
                                    ), taskExecutor);
                        }
                    } catch (BrokerException e){
                        rtnCode = e.code();
                        log.error("staking processRedeemRequest error:{},param:{}", e, stakingProductOrder.toString());
                    }
                }
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, redeemLock);
            }
        }
        return rtnCode;
    }

    /**
     * 赎回处理
     *
     * @param stakingRedeemEvent 赎回
     * @return
     */
    public Integer processRedeemEvent(StakingRedeemEvent stakingRedeemEvent) {
        Integer processRtn = BrokerErrorCode.SUCCESS.code();
        boolean lock;

        String redeemStatusKey = String.format(StakingConstant.REDEEM_STATUS_CODE_PREFIX
                , stakingRedeemEvent.getOrgId()
                , stakingRedeemEvent.getUserId()
                , stakingRedeemEvent.getProductId()
                , stakingRedeemEvent.getTransferId());

        String redeemLock = String.format(StakingConstant.STAKING_REDEEM_EVENT_LOCK_KEY
                , stakingRedeemEvent.getOrgId()
                , stakingRedeemEvent.getUserId()
                , stakingRedeemEvent.getProductId()
                , stakingRedeemEvent.getAssetId());

        lock = RedisLockUtils.tryLock(redisTemplate, redeemLock, StakingConstant.STAKING_REDEEM_LOCK_EXPIRE_SECOND * 30);
        if (!lock) {
            return BrokerErrorCode.REQUEST_TOO_FAST.code();
        }

        try {
            Long currentTime = System.currentTimeMillis();
            StakingProductOrder productOrder = stakingProductOrderMapper.getByTransferId(stakingRedeemEvent.getOrgId()
                    , stakingRedeemEvent.getUserId()
                    , stakingRedeemEvent.getProductId()
                    , stakingRedeemEvent.getTransferId());
            if (productOrder != null) {
                // no transfer
                if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE) {
                    // validation
                    processRtn = redeemValidation(stakingRedeemEvent.getAssetId(), stakingRedeemEvent.getOrgId(), BigDecimal.ZERO);
                    if (processRtn == BrokerErrorCode.SUCCESS.code()) {
                        processRtn = redeemTransferAndUpdate(productOrder, currentTime);
                    }
                } else if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE) {
                    log.warn("staking order status is faild:orderId->{}", productOrder.getId());
                    processRtn = BrokerErrorCode.FINANCE_REDEEM_ERROR.code();
                }
            } else {
                processRtn = BrokerErrorCode.FINANCE_REDEEM_RECORD_NOT_EXIST.code();
            }
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, redeemLock);
        }

        setStatusCode(redeemStatusKey, processRtn.toString());
        return processRtn;
    }

    /**
     * 内部计算利息
     *
     * @param productRebate  理财产品派息设置
     * @param stakingProduct 理财产品信息
     * @param currentTime    时间
     */
    @Override
    public void calcInterest(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime) {
        calcCurrentRebate(productRebate, stakingProduct, currentTime);
    }

    /**
     * 后台发起计算利息
     *
     * @param productRebate  理财产品派息设置
     * @param stakingProduct 理财产品信息
     * @param currentTime    时间
     */
    public void reCalcInterest(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime) {
        if (productRebate.getType() == StakingProductRebateType.STPD_REBATE_TYPE_PRINCIPAL_VALUE) {
            // 还本
        } else if (productRebate.getType() == StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE) {
            // 计息
        }
    }

    /**
     * 派利息
     *
     * @param stakingTransferEvent 派息消息
     */
    @Override
    public void dispatchInterest(StakingTransferEvent stakingTransferEvent) {
        if (stakingTransferEvent.getRebateType() == StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE) {
            // 派息
            dispatchInterestMethod(stakingTransferEvent);
        } else if (stakingTransferEvent.getRebateType() == StakingProductRebateType.STPD_REBATE_TYPE_PRINCIPAL_VALUE) {
            // 还本
            returnPrincipal(stakingTransferEvent);
        }
    }

    /**
     * 派息
     *
     * @param stakingTransferEvent
     */
    private void dispatchInterestMethod(StakingTransferEvent stakingTransferEvent) {
        boolean runStatus = true;
        Long currentTime = System.currentTimeMillis();
        List<StakingProductRebateDetail> productRebateDetailList;

        StakingProductRebate stakingProductRebate = productRebateMapper.selectOne(StakingProductRebate.builder()
                .orgId(stakingTransferEvent.getOrgId())
                .productId(stakingTransferEvent.getProductId())
                .id(stakingTransferEvent.getRebateId())
                .build());
        if (stakingProductRebate == null || (stakingProductRebate.getStatus() != StakingProductRebateStatus.STPD_REBATE_STATUS_CALCED_VALUE)) {
            log.warn("staking current dispatch interest,rebate is not exists or status is error:org->{},product->{},rebate->{},status->{}"
                    , stakingTransferEvent.getOrgId(), stakingTransferEvent.getProductId()
                    , stakingTransferEvent.getRebateId(), stakingProductRebate == null ? -1 : stakingProductRebate.getStatus());
            return;
        }

        while (runStatus) {
            productRebateDetailList = productRebateDetailMapper.getWatingTransferList(stakingTransferEvent.getOrgId()
                    , stakingTransferEvent.getProductId()
                    , StakingProductType.FI_CURRENT.getNumber()
                    , stakingTransferEvent.getRebateId());
            if (CollectionUtils.isEmpty(productRebateDetailList)) {
                runStatus = false;
                // 更新该派息设置已派息完成
                productRebateMapper.updateStatus(stakingTransferEvent.getOrgId(), stakingTransferEvent.getRebateId()
                        , StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS.getNumber()
                        , currentTime);
            } else {
                // 逐一转账
                for (StakingProductRebateDetail productRebateDetail : productRebateDetailList) {
                    EnumStakingTransfer enumStakingTransfer = dividendTransfer(productRebateDetail);
                    if (enumStakingTransfer == EnumStakingTransfer.SUCCESS) {
                        // update status
                        productStrategyService.updateCurrentRebateDetailStatus(productRebateDetail);
                    } else{
                        // transfer failed
                        return;
                    }
                }
            }
        }
    }

    /**
     * 还本
     *
     * @param stakingTransferEvent
     */
    private void returnPrincipal(StakingTransferEvent stakingTransferEvent) {
        long maxId = 0;
        int transferCounter = 0;
        while (true) {
            int rtnCode = 0;
            try {
                // 1. 获取还本赎回订单
                List<StakingProductOrder> stakingProductOrderList = stakingProductOrderMapper.listCurrentRedeemPrincipalOrders(stakingTransferEvent.getOrgId()
                        , stakingTransferEvent.getProductId(), maxId, StakingConstant.BATCH_LIST_SIZE);
                if (transferCounter > TRANSFER_FAILED_COUNTER || CollectionUtils.isEmpty(stakingProductOrderList)) {
                    break;
                }
                for (StakingProductOrder stakingProductOrder : stakingProductOrderList) {
                    // 2. 循环转账、扣减资产
                    rtnCode = redeemTransferAndUpdate(stakingProductOrder, System.currentTimeMillis());
                    if (rtnCode != BrokerErrorCode.SUCCESS.code()) {
                        transferCounter++;
                        break;
                    }
                }

                maxId = stakingProductOrderList.stream().mapToLong(StakingProductOrder::getId).max().orElse(0L);
            } catch (Exception ex){
                log.error("staking product current strategy returnPrincipal, param:{} error:{}"
                        , stakingTransferEvent.toString(), ex.getMessage());
                return;
            }
        }
    }

    @Override
    public List<StakingSchema> listProducts(StakingSchema stakingSchema) {
        return null;
    }

    /**
     * 赎回转账 and Update
     *
     * @param stakingProductOrder
     * @param currentTime
     * @return
     */
    private Integer redeemTransferAndUpdate(StakingProductOrder stakingProductOrder, Long currentTime) {
        int rtnCode = BrokerErrorCode.SUCCESS.code();
        try {
            // transfer
            EnumStakingTransfer enumStakingTransfer = redeemTransfer(stakingProductOrder);
            if (enumStakingTransfer == EnumStakingTransfer.FAIL) {
                // update redeem failed 并且归还资产
                // by songxd on 2021-07-05,赎回转账失败，报警提示，订单保持赎回中
                // productStrategyService.redeemOrderFailAndReturnAsset(stakingProductOrder, 0);

                // send message to slack
                List<String> params = Arrays.asList(stakingProductOrder.getProductId().toString()
                        ,stakingProductOrder.getPayAmount().stripTrailingZeros().toPlainString());
                brokerSlackUtil.sendAlertMsg(BrokerSlackUtil.AlertKey.STAKING_REDEEM_TRANSFER_FAILD, params);

                // transfer failed
                rtnCode = BrokerErrorCode.STAKING_REDEEM_TRANSFER_WAIT.code();
            } else if(enumStakingTransfer == EnumStakingTransfer.SUCCESS){
                // 更新赎回单据状态为赎回成功，然后释放产品额度
                productStrategyService.updateOrderStatusAndReleaseSold(stakingProductOrder, 0, currentTime);
            } else{
                rtnCode = BrokerErrorCode.STAKING_PROCESSING.code();
            }
        } catch (BrokerException ex) {
            log.error("redeemTransferAndUpdate order:{}  exception:{}", stakingProductOrder.toString(), ex);
            rtnCode = ex.code();
        } catch (Exception ex) {
            log.error("redeemTransferAndUpdate order:{}  exception:{}", stakingProductOrder.toString(), ex);
            rtnCode = BrokerErrorCode.DB_ERROR.code();
        }
        return rtnCode;
    }

    /**
     * 计算活期利息
     *
     * @param productRebate
     * @param stakingProduct
     * @param currentTime
     */
    private void calcCurrentRebate(StakingProductRebate productRebate, StakingProduct stakingProduct, Long currentTime) {
        List<StakingAssetSnapshot> assetSnapshotsList;
        boolean grandTotal = true;
        long maxId = 0L;
        long maxUserId = 0L;

        // 获取最新的已派息设置
        StakingProductRebate preProductRebate = productRebateMapper.getPreStakingProductRebate(productRebate.getOrgId(),
                productRebate.getProductId()
                , productRebate.getId(),
                productRebate.getProductType()
                , productRebate.getRebateDate());

        while (true) {
            // 获取上次派息和本次派息之间的申购、赎回数据、按照用户、产品、机构合计金额
            List<UserCurrentOrderSum> listCurrentSum = stakingProductOrderMapper.listUserCurrentOrderSum(productRebate.getOrgId(), productRebate.getProductId()
                    , maxUserId, preProductRebate == null ? 0L : preProductRebate.getRebateDate(), productRebate.getRebateDate()
                    , productRebate.getId()
                    , StakingConstant.BATCH_LIST_SIZE);

            if (CollectionUtils.isEmpty(listCurrentSum)) {
                break;
            }

            // batch insert tb_staking_asset_snapshot ignore by orgId,productId,productRebateId,userId
            List<StakingAssetSnapshot> listSnapshot = new ArrayList<>();
            for (UserCurrentOrderSum userCurrentOrderSum : listCurrentSum) {
                listSnapshot.add(StakingAssetSnapshot.builder()
                        .orgId(productRebate.getOrgId())
                        .userId(userCurrentOrderSum.getUserId())
                        .productId(userCurrentOrderSum.getProductId())
                        .tokenId(stakingProduct.getTokenId())
                        .dailyDate(productRebate.getRebateDate())
                        .netAsset(userCurrentOrderSum.getAmount())
                        .payInterest(BigDecimal.ZERO)
                        .apr(productRebate.getRebateRate())
                        .status(0)
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .productRebateId(productRebate.getId())
                        .build());
            }
            try {
                stakingAssetSnapshotBatchMapper.insertList(listSnapshot);
            } catch (Exception ex) {
                log.error("staking calcCurrentRebate1 insertList error->{},params->{}", ex, productRebate.toString());
                return;
            }
            maxUserId = listCurrentSum.stream().mapToLong(UserCurrentOrderSum::getUserId).max().orElse(0L);
        }

        if (preProductRebate != null) {
            while (grandTotal) {
                // 1. 获取活期资产列表
                assetSnapshotsList = assetSnapshotMapper.listPreDateData(preProductRebate.getOrgId()
                        , preProductRebate.getProductId()
                        , preProductRebate.getId()
                        , productRebate.getId()
                        , maxId
                        , StakingConstant.BATCH_LIST_SIZE);
                if (CollectionUtils.isEmpty(assetSnapshotsList)) {
                    // calc end
                    grandTotal = false;
                    continue;
                }
                try {
                    List<StakingProductRebateDetail> listRebateDetail = new ArrayList<>();
                    List<StakingAssetSnapshot> listInsertAssetSnapshot = new ArrayList<>();
                    List<StakingAssetSnapshot> listUpdateAssetSnapshot = new ArrayList<>();
                    maxId = assetSnapshotsList.stream().mapToLong(StakingAssetSnapshot::getId).max().orElse(0L);

                    for (StakingAssetSnapshot assetSnapshot : assetSnapshotsList) {
                        assetSnapshot.setDailyDate(productRebate.getRebateDate());
                        assetSnapshot.setProductRebateId(productRebate.getId());
                        // calc interest
                        // calcInterestByRebate(preProductRebate,stakingProduct, assetSnapshot.getNetAsset(), BigDecimal.ONE)
                        assetSnapshot.setPayInterest(assetSnapshot.getNetAsset().multiply(assetSnapshot.getApr())
                                .divide(StakingConstant.DAY_OF_YEAR, StakingConstant.INTEREST_SCALE, RoundingMode.DOWN));
                        // set current apr
                        assetSnapshot.setApr(productRebate.getRebateRate());
                        assetSnapshot.setCreatedAt(currentTime);
                        assetSnapshot.setUpdatedAt(currentTime);
                        Long accountId = accountService.getAccountId(assetSnapshot.getOrgId(), assetSnapshot.getUserId());
                        listRebateDetail.add(StakingProductRebateDetail.builder()
                                .orgId(assetSnapshot.getOrgId())
                                .userId(assetSnapshot.getUserId())
                                .productId(assetSnapshot.getProductId())
                                .productType(stakingProduct.getType())
                                .orderId(assetSnapshot.getId())
                                .productRebateId(assetSnapshot.getProductRebateId())
                                .tokenId(preProductRebate.getTokenId())
                                .userAccountId(accountId)
                                .originAccountId(stakingProduct.getDividendAccountId())
                                .transferId(sequenceGenerator.getLong())
                                .rebateType(StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_INTEREST_VALUE)
                                .rebateAmount(assetSnapshot.getPayInterest())
                                .status(StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_WAITING_VALUE)
                                .createdAt(currentTime)
                                .updatedAt(currentTime)
                                .build());
                        StakingAssetSnapshot userAssetSnapshot = isExistsUserAssetSnapshot(assetSnapshot);
                        if (userAssetSnapshot == null) {
                            listInsertAssetSnapshot.add(assetSnapshot);
                        } else {
                            assetSnapshot.setId(userAssetSnapshot.getId());
                            listUpdateAssetSnapshot.add(assetSnapshot);
                        }
                    }
                    productStrategyService.insertOrUpdateAssetSnapshotAndInsertRebateDetail(listInsertAssetSnapshot, listUpdateAssetSnapshot, listRebateDetail);
                } catch (Exception ex) {
                    log.error("staking calcCurrentRebate2 insertOrUpdateAssetSnapshotAndInsertRebateDetail error->{},params->{}", ex, productRebate.toString());
                    return;
                }
            }
        }

        try {
            // 5. 更新本次派息状态，总派息金额
            updateStakingProductRebateStatusAndAmountOrRate(productRebate, stakingProduct, 1L, false, null, currentTime);
        } catch (Exception ex) {
            log.error("staking current updateStakingProductRebateStatusAndAmountOrRate：{},StakingProductRebate:{} ", ex, productRebate.toString());
        }
    }

    /**
     * 判断该批次的用户资产快照是否存在
     *
     * @param assetSnapshot
     * @return
     */
    private StakingAssetSnapshot isExistsUserAssetSnapshot(StakingAssetSnapshot assetSnapshot) {
        return assetSnapshotMapper.selectOne(StakingAssetSnapshot.builder()
                .orgId(assetSnapshot.getOrgId())
                .productId(assetSnapshot.getProductId())
                .userId(assetSnapshot.getUserId())
                .productRebateId(assetSnapshot.getProductRebateId())
                .build());
    }

    /**
     * 赎回转账
     *
     * @param stakingProductOrder 理财赎回订单
     * @return
     */
    private EnumStakingTransfer redeemTransfer(StakingProductOrder stakingProductOrder) {

        StakingProduct stakingProduct = productStrategyService.getStakingProduct(stakingProductOrder.getOrgId(), stakingProductOrder.getProductId());
        if (stakingProduct == null) {
            return EnumStakingTransfer.FAIL;
        }

        StakingTransferRecord transferRecord = StakingTransferRecord.builder()
                .orgId(stakingProductOrder.getOrgId())
                .tokenId(stakingProductOrder.getTokenId())
                .amount(stakingProductOrder.getPayAmount().abs())
                .transferId(stakingProductOrder.getTransferId())

                .sourceOrgId(stakingProductOrder.getOrgId())
                .sourceAccountId(stakingProduct.getPrincipalAccountId())
                .sourceAccountType(AccountType.PAY_PRINCIPAL_ACCOUNT)
                .sourceBusinessSubject(BusinessSubject.PRINCIPAL_WEALTH_MANAGEMENT_PRODUCTS)

                .targetOrgId(stakingProductOrder.getOrgId())
                .targetAccountId(stakingProductOrder.getAccountId())
                .targetAccountType(AccountType.GENERAL_ACCOUNT)
                .targetBusinessSubject(BusinessSubject.PRINCIPAL_WEALTH_MANAGEMENT_PRODUCTS)
                .build();

        return stakingTransferService.transfer(transferRecord, 0);
    }

    /**
     * 赎回验证
     *
     * @param assetId 资产记录ID
     * @param orgId   机构ID
     * @param amount  赎回金额
     * @return
     */
    private Integer redeemValidation(Long assetId, Long orgId, BigDecimal amount) {
        StakingAsset stakingAsset = stakingAssetMapper.selectOne(StakingAsset.builder().id(assetId).orgId(orgId).build());
        if (stakingAsset == null) {
            log.warn("staking redeem asset is null:orgid->{},assetId->{}", orgId, assetId);
            return BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code();
        }
        BigDecimal userOrderAmount = sumUserAsset(stakingAsset.getOrgId(), stakingAsset.getProductId(), stakingAsset.getUserId());
        if (stakingAsset.getCurrentAmount().compareTo(userOrderAmount) != 0) {
            log.warn("staking user asset validation failed");
            return BrokerErrorCode.FINANCE_REDEEM_ERROR.code();
        } else if (stakingAsset.getCurrentAmount().compareTo(amount) < 0) {
            log.warn("staking user asset insufficient");
            return BrokerErrorCode.FINANCE_REDEEM_INSUFFICIENT_BALANCE.code();
        }
        return BrokerErrorCode.SUCCESS.code();
    }

    /**
     * 获取用户累计净资产金额
     *
     * @param orgId     机构ID
     * @param productId 产品ID
     * @param userId    用户ID
     * @return 合计资产金额
     */
    public BigDecimal sumUserAsset(Long orgId, Long productId, Long userId) {
        BigDecimal userAsset = BigDecimal.ZERO;
        StakingAssetSnapshot assetSnapshot = assetSnapshotMapper.getLastAssetSnapshot(orgId, productId, userId);
        if (assetSnapshot != null) {
            userAsset = userAsset.add(assetSnapshot.getNetAsset());
        }

        // 申购订单金额：申购成功
        BigDecimal userDayChange = stakingProductOrderMapper.sumOrderAmountByUserId(orgId, productId, userId, assetSnapshot != null ? assetSnapshot.getDailyDate() : 0L);
        if (userDayChange != null) {
            userAsset = userAsset.add(userDayChange);
        }

        // 赎回订单金额：赎回成功 + 赎回中的合计金额
        BigDecimal userRedeemChange = stakingProductOrderMapper.sumRedeemOrderAmountByUserId(orgId, productId, userId, assetSnapshot != null ? assetSnapshot.getDailyDate() : 0L);
        if (userRedeemChange != null) {
            userAsset = userAsset.add(userRedeemChange);
        }
        return userAsset;
    }

    /**
     * 数据验证
     *
     * @return
     */
    public Boolean dataVerify(Long orgId, Long productId) {
        boolean flag = false;
        // 1.验证用户订单明细合计金额和当前资产金额是否一致
        // 获取用户订单累计金额和用户当前资产金额
        List<StakingAssetOrderAmount> listAssetOrderAmount = stakingAssetMapper.queryStakingAssetOrderAmount(orgId, productId);
        if (CollectionUtils.isEmpty(listAssetOrderAmount)) {
            flag = true;
        } else {
            for (StakingAssetOrderAmount assetOrderAmount : listAssetOrderAmount) {
                // 历史遗漏差异，忽略
                if (assetOrderAmount.getProductId().equals(732371163958744352L) && assetOrderAmount.getUserId().equals(384092901136954880L)) {
                    BigDecimal subAmount = assetOrderAmount.getAssetAmount().subtract(assetOrderAmount.getOrderAmount());
                    flag = subAmount.abs().compareTo(new BigDecimal("1029")) == 0;
                } else if (assetOrderAmount.getProductId().equals(732371163958744350L) &&
                        (assetOrderAmount.getUserId().equals(394916763437150464L) || assetOrderAmount.getUserId().equals(241292601066782720L))) {
                    // 历史遗漏差异，忽略
                    BigDecimal subAmount = assetOrderAmount.getAssetAmount().subtract(assetOrderAmount.getOrderAmount());
                    flag = subAmount.abs().compareTo(new BigDecimal("0.01")) == 0;
                } else {
                    flag = false;
                }
            }
        }

        // 2.验证派息金额是否一致
        return flag;
    }

    /**
     * 申购补偿
     *
     * @param stakingProductOrder  失败订单
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
                productStrategyService.updateOrderStatusAndInsertAsset(stakingProductOrder, 2, System.currentTimeMillis());
            }
        } catch (Exception ex) {
            log.error("staking current subscribeRedress:{}  error:{}", stakingProductOrder.toString(), ex);
        }
    }

    /**
     * 赎回补偿
     *
     * @param stakingProductOrder  失败订单
     * @return
     */
    public void redeemRedress(StakingProductOrder stakingProductOrder) {
        try {
            // transfer
            EnumStakingTransfer enumStakingTransfer = redeemTransfer(stakingProductOrder);
            if (enumStakingTransfer == EnumStakingTransfer.FAIL) {
                // update redeem failed
                productStrategyService.redeemOrderFailAndReturnAsset(stakingProductOrder, 0);
            } else if(enumStakingTransfer == EnumStakingTransfer.SUCCESS){
                // 更新赎回单据状态为赎回成功,释放产品额度
                productStrategyService.updateOrderStatusAndReleaseSold(stakingProductOrder, 2, System.currentTimeMillis());
            }
        } catch (Exception ex) {
            log.error("staking current redeemRedress:{}  exception:{}", stakingProductOrder.toString(), ex);
        }
    }
}
