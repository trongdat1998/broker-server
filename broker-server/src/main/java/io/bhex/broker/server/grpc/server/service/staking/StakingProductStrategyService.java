package io.bhex.broker.server.grpc.server.service.staking;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.EnumBusinessLimit;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.domain.staking.StakingSchema;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.BaseBizConfigService;
import io.bhex.broker.server.grpc.server.service.BusinessLimitService;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import javax.persistence.criteria.CriteriaBuilder;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 提供理财产品相关的方法
 *
 * @author songxd
 * @date
 */
@Service
@Slf4j
public class StakingProductStrategyService {

    @Resource
    private StakingAssetMapper stakingAssetMapper;

    @Resource
    private StakingAssetSnapshotMapper stakingAssetSnapshotMapper;

    @Resource
    private StakingAssetSnapshotBatchMapper stakingAssetSnapshotBatchMapper;

    @Resource
    private StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    private StakingProductMapper stakingProductMapper;

    @Resource
    private StakingProductRebateDetailMapper productRebateDetailMapper;

    @Resource
    private StakingProductRebateMapper stakingProductRebateMapper;

    @Resource
    private StakingProductRebateDetailBatchMapper productRebateDetailBatchMapper;

    @Resource
    private ISequenceGenerator<Long> sequenceGenerator;

    @Resource
    private StakingProductJourMapper stakingProductJourMapper;

    @Resource
    private AccountService accountService;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private BusinessLimitService businessLimitService;

    private final Cache<Long, StakingProduct> STAKING_PRODUCT_CACHE = CacheBuilder
            .newBuilder()
            .expireAfterWrite(10L, TimeUnit.MINUTES)
            .build();

    /**
     * 保存申购订单并更新理财产品的销售数量
     *
     * @param productOrder 申购订单
     * @return true:success false:fail
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public Boolean insertOrderAndUpdateProductSoldLots(StakingProductOrder productOrder) {
        boolean rtnCode;
        // insert order
        int insertCount = stakingProductOrderMapper.insertSelective(productOrder);
        rtnCode = insertCount > 0;
        if (!rtnCode) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PURCHASE_ERROR);
        } else {
            // 2. update staking product sold amount
            int rowCount = stakingProductMapper.updateStakingProductSoldLots(productOrder.getProductId(), productOrder.getOrgId()
                    , productOrder.getPayLots(), System.currentTimeMillis());
            rtnCode = rowCount > 0;
            if (!rtnCode) {
                throw new BrokerException(BrokerErrorCode.FINANCE_TOTAL_LIMIT_OUT);
            }
        }
        return true;
    }

    /**
     * 申购失败：设置申购订单状态为失败，同时释放理财产品的可申购数量
     *
     * @param stakingProductOrder 申购订单
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void updateOrderFailAndProductSoldLots(StakingProductOrder stakingProductOrder, Integer orderStatus) {
        long currentTime = System.currentTimeMillis();
        // update subscribe failed
        stakingProductOrderMapper.updateStakingProductOrderStatus(stakingProductOrder.getOrgId(), stakingProductOrder.getId()
                , StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE, orderStatus, currentTime);

        // 申购总数还源
        stakingProductMapper.updateStakingProductSoldLots(stakingProductOrder.getProductId(), stakingProductOrder.getOrgId()
                , (-stakingProductOrder.getPayLots()), currentTime);
    }

    /**
     * 更新申购订单状态为申购成功
     * 保存理财资产：如果理财项目已经存在则更新，否则新增
     * 更新理财产品已售金额
     * 如果是活期则记录当日资产快照
     *
     * @param productOrder 申购订单
     * @param currentTime  时间戳
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void updateOrderStatusAndInsertAsset(StakingProductOrder productOrder, Integer orderStatus, Long currentTime) {
        try {
            // 1.update order status success
            int rtnRows = stakingProductOrderMapper.updateStakingProductOrderSuccess(productOrder.getOrgId()
                    , productOrder.getId(), orderStatus, currentTime);
            if (rtnRows != 0) {
                // 2.staking asset insert or update
                StakingAsset stakingAsset = stakingAssetMapper.selectOne(StakingAsset.builder()
                        .orgId(productOrder.getOrgId())
                        .userId(productOrder.getUserId())
                        .productId(productOrder.getProductId())
                        .build());
                if (stakingAsset == null) {
                    // 2.1 insert
                    stakingAssetMapper.insertSelective(StakingAsset.builder()
                            .id(sequenceGenerator.getLong())
                            .orgId(productOrder.getOrgId())
                            .userId(productOrder.getUserId())
                            .productId(productOrder.getProductId())
                            .productType(productOrder.getProductType())
                            .accountId(productOrder.getAccountId())
                            .tokenId(productOrder.getTokenId())
                            .totalAmount(productOrder.getPayAmount())
                            .lastProfit(BigDecimal.ZERO)
                            .totalProfit(BigDecimal.ZERO)
                            .currentAmount(productOrder.getPayAmount())
                            .createdAt(currentTime)
                            .updatedAt(currentTime)
                            .build());
                } else {
                    // 2.2 update
                    stakingAssetMapper.updateStakingAsset(productOrder.getOrgId(), productOrder.getUserId(), productOrder.getProductId(),
                            productOrder.getPayAmount(), currentTime);
                }

                // 3. 理财流水记录
                stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                        .id(productOrder.getTransferId())
                        .orgId(productOrder.getOrgId())
                        .userId(productOrder.getUserId())
                        .accountId(productOrder.getAccountId())
                        .productId(productOrder.getProductId())
                        .productType(productOrder.getProductType())
                        .tokenId(productOrder.getTokenId())
                        .type(StakingProductJourType.STPD_JOUR_TYPE_SUBSCRIBE_VALUE)
                        .amount(productOrder.getPayAmount())
                        .transferId(productOrder.getTransferId())
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build());
            }
        } catch (Exception ex) {
            log.error("staking updateOrderStatusAndInsertAsset error:{},Message:{}", ex, productOrder.toString());
            throw ex;
        }
    }

    /**
     * 保存赎回订单 减少项目资产金额
     *
     * @param stakingProductOrder 赎回订单
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public Boolean insertRedeemOrderAndReduceAsset(StakingProductOrder stakingProductOrder) throws BrokerException {
        boolean flag = false;
        // 1.update order status success
        int rtnRows = stakingProductOrderMapper.insertSelective(stakingProductOrder);
        if (rtnRows != 0) {
            // 2.reduce staking asset
            int updateRtn = stakingAssetMapper.reduceStakingAsset(stakingProductOrder.getOrgId(), stakingProductOrder.getUserId(), stakingProductOrder.getProductId(),
                    stakingProductOrder.getPayAmount().abs(), System.currentTimeMillis());
            if (updateRtn == 0) {
                throw new BrokerException(BrokerErrorCode.FINANCE_REDEEM_INSUFFICIENT_BALANCE);
            }
            flag = true;
        }
        return flag;
    }

    /**
     * 保存赎回订单 减少项目资产金额
     *
     * @param stakingProductOrder 赎回订单
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public Boolean redeemOrderFailAndReturnAsset(StakingProductOrder stakingProductOrder, Integer orderStatus) throws BrokerException {
        boolean flag = false;
        // 1.update order status success
        int rtnRows = stakingProductOrderMapper.updateStakingProductOrderStatus(stakingProductOrder.getOrgId(), stakingProductOrder.getId()
                , StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE, orderStatus, System.currentTimeMillis());
        if (rtnRows != 0) {
            // 2.return staking asset
            stakingAssetMapper.returnStakingAsset(stakingProductOrder.getOrgId(), stakingProductOrder.getUserId(), stakingProductOrder.getProductId(),
                    stakingProductOrder.getPayAmount().abs(), System.currentTimeMillis());
            flag = true;
        }
        return flag;
    }

    /**
     * 更新赎回订单状态为赎回成功
     * 减少该项目资产金额
     *
     * @param stakingProductOrder 赎回订单
     * @param orderStatus         0=申购 2=失败
     * @param currentTime         时间戳
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void updateOrderStatusAndReleaseSold(StakingProductOrder stakingProductOrder, Integer orderStatus, Long currentTime)  {

        // 1.update order status success
        int rtnRows = stakingProductOrderMapper.updateStakingProductOrderSuccess(stakingProductOrder.getOrgId(), stakingProductOrder.getId(), orderStatus, currentTime);
        if (rtnRows != 0) {
            // 2. 释放产品可售数量
            stakingProductMapper.releaseStakingProductSoldLots(stakingProductOrder.getProductId(), stakingProductOrder.getOrgId()
                    , stakingProductOrder.getPayLots(), System.currentTimeMillis());
            // 3. 理财流水赎回记录
            stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                    .id(stakingProductOrder.getTransferId())
                    .orgId(stakingProductOrder.getOrgId())
                    .userId(stakingProductOrder.getUserId())
                    .accountId(stakingProductOrder.getAccountId())
                    .productId(stakingProductOrder.getProductId())
                    .productType(StakingProductType.FI_CURRENT_VALUE)
                    .tokenId(stakingProductOrder.getTokenId())
                    .type(StakingProductJourType.STPD_JOUR_TYPE_REDEEM_VALUE)
                    .amount(stakingProductOrder.getPayAmount().abs())
                    .transferId(stakingProductOrder.getTransferId())
                    .createdAt(currentTime)
                    .updatedAt(currentTime)
                    .build());
            // 4.如果是活期理财，并且是到期还本订单
            if(stakingProductOrder.getOrderType() == StakingProductOrderType.STPD_ORDER_TYPE_REDEEM_PRINCIPAL_VALUE
                    && stakingProductOrder.getProductType() == StakingProductType.FI_CURRENT_VALUE){
                StakingAssetSnapshot lastAssetSnapshot = stakingAssetSnapshotMapper.getLastAssetSnapshot(stakingProductOrder.getOrgId()
                        , stakingProductOrder.getProductId(), stakingProductOrder.getUserId());
                if(lastAssetSnapshot != null){
                    // reduce snapshot
                    stakingAssetSnapshotMapper.updateAssetSnapshot(stakingProductOrder.getOrgId(), lastAssetSnapshot.getId()
                            , stakingProductOrder.getPayAmount(), System.currentTimeMillis());
                }
            }
        }
    }

    /**
     * 理财产品：定期
     * 更新派息记录状态为'已派息'
     * 更新申购订单的最后派息日期
     * 如果是还本金则更新订单状态为已赎回,同时减少用户该产品的理财资产
     *
     * @param productRebateDetail 理财产品派息记录
     */
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void updateTimeRebateDetailStatus(StakingProductRebateDetail productRebateDetail) {

        Long currentTime = System.currentTimeMillis();

        try {

            // 1. 修改派息记录状态为"已转账"
            productRebateDetailMapper.updateStakingRebateDetailStatus(productRebateDetail.getOrgId(), productRebateDetail.getId()
                    , StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_SUCCESS_VALUE
                    , currentTime);

            // 2. 修改申购订单的最后派息日期
            stakingProductOrderMapper.updateStakingProductOrderLastInterestDate(productRebateDetail.getOrgId()
                    , productRebateDetail.getOrderId()
                    , StakingUtils.getDayDate(currentTime)
                    , currentTime);

            // 3. 如果是本金转账记录，则修改该订单状态为已赎回，同时减少用户理财资产
            if (productRebateDetail.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_PRINCIPAL_VALUE) {

                // 修改理财申购订单的状态为已赎回
                stakingProductOrderMapper.updateStakingProductRedeemStatusAndDate(productRebateDetail.getOrgId(), productRebateDetail.getOrderId()
                        , StakingProductOrderStatus.STPD_ORDER_STATUS_REDEEMED_VALUE
                        , currentTime
                        , currentTime);

                // TODO 检查该订单是否设置了自动转活期理财

                // 扣除用户理财资产
                stakingAssetMapper.reduceStakingAsset(productRebateDetail.getOrgId()
                        , productRebateDetail.getUserId()
                        , productRebateDetail.getProductId()
                        , productRebateDetail.getRebateAmount()
                        , currentTime);

                // 4. 理财还本流水记录
                stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                        .id(productRebateDetail.getTransferId())
                        .orgId(productRebateDetail.getOrgId())
                        .userId(productRebateDetail.getUserId())
                        .accountId(productRebateDetail.getUserAccountId())
                        .productId(productRebateDetail.getProductId())
                        .productType(productRebateDetail.getProductType())
                        .tokenId(productRebateDetail.getTokenId())
                        .type(StakingProductJourType.STPD_JOUR_TYPE_REDEEM_VALUE)
                        .amount(productRebateDetail.getRebateAmount())
                        .transferId(productRebateDetail.getTransferId())
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build());
            } else {
                // 更新用户理财资产中的总收益
                stakingAssetMapper.updateStakingProfit(productRebateDetail.getOrgId()
                        , productRebateDetail.getUserId()
                        , productRebateDetail.getProductId()
                        , productRebateDetail.getRebateAmount()
                        , currentTime);

                // 4. 理财付息流水记录
                stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                        .id(productRebateDetail.getTransferId())
                        .orgId(productRebateDetail.getOrgId())
                        .userId(productRebateDetail.getUserId())
                        .accountId(productRebateDetail.getUserAccountId())
                        .productId(productRebateDetail.getProductId())
                        .productType(productRebateDetail.getProductType())
                        .tokenId(productRebateDetail.getTokenId())
                        .type(StakingProductJourType.STPD_JOUR_TYPE_INTEREST_VALUE)
                        .amount(productRebateDetail.getRebateAmount())
                        .transferId(productRebateDetail.getTransferId())
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build());
            }
        } catch (Exception ex) {
            log.error("staking updateTimeRebateDetailStatus error:{}, productRebateDetail:{}", ex, productRebateDetail.toString());
            throw ex;
        }
    }

    /**
     * 理财产品：活期
     * 更新派息记录状态为'已派息'
     * 更新理财资产中的累计收益
     *
     * @param productRebateDetail 理财产品派息记录
     */
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void updateCurrentRebateDetailStatus(StakingProductRebateDetail productRebateDetail) {

        Long currentTime = System.currentTimeMillis();

        try {
            // 1. 修改派息记录状态为"已转账"
            productRebateDetailMapper.updateStakingRebateDetailStatus(productRebateDetail.getOrgId(), productRebateDetail.getId()
                    , StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_SUCCESS_VALUE
                    , currentTime);

            // 2. 更新用户理财资产中的总收益
            stakingAssetMapper.updateStakingProfit(productRebateDetail.getOrgId()
                    , productRebateDetail.getUserId()
                    , productRebateDetail.getProductId()
                    , productRebateDetail.getRebateAmount()
                    , currentTime);

            // 3. 理财流水记录
            stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                    .id(productRebateDetail.getTransferId())
                    .orgId(productRebateDetail.getOrgId())
                    .userId(productRebateDetail.getUserId())
                    .accountId(productRebateDetail.getUserAccountId())
                    .productId(productRebateDetail.getProductId())
                    .productType(StakingProductType.FI_CURRENT_VALUE)
                    .tokenId(productRebateDetail.getTokenId())
                    .type(StakingProductJourType.STPD_JOUR_TYPE_INTEREST_VALUE)
                    .amount(productRebateDetail.getRebateAmount().abs())
                    .transferId(productRebateDetail.getTransferId())
                    .createdAt(currentTime)
                    .updatedAt(currentTime)
                    .build());

        } catch (Exception ex) {
            log.error("staking updateCurrentRebateDetailStatus error:{}, productRebateDetail:{}", ex, productRebateDetail);
            throw ex;
        }
    }

    /**
     * 理财产品：活期
     * 更新记录活期资产快照 新增 派息记录
     *
     * @param assetSnapshotInsertList        新增活期资产快照数据
     * @param assetSnapshotUpdateList        更新活期资产快照数据
     * @param stakingProductRebateDetailList 活期理财项目派息记录
     */
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void insertOrUpdateAssetSnapshotAndInsertRebateDetail(List<StakingAssetSnapshot> assetSnapshotInsertList
            , List<StakingAssetSnapshot> assetSnapshotUpdateList
            , List<StakingProductRebateDetail> stakingProductRebateDetailList) {
        String assetSnapshotIds = "";
        try {
            if (! CollectionUtils.isEmpty(assetSnapshotInsertList)) {
                List<Long> ids = assetSnapshotInsertList.stream().map(StakingAssetSnapshot::getUserId).collect(Collectors.toList());
                assetSnapshotIds = StringUtils.join(ids, ",");
                // 1. batch insert
                stakingAssetSnapshotBatchMapper.insertList(assetSnapshotInsertList);
            }
            if (! CollectionUtils.isEmpty(assetSnapshotUpdateList)) {
                // 2. update todo 根据数据量，可改为批量更新 stakingAssetSnapshotMapper.batchUpdate
                assetSnapshotUpdateList.forEach(assetSnapshot -> {
                    stakingAssetSnapshotMapper.updateChangeAssetSnapshot(assetSnapshot);
                });
            }
            // 3. productRebateDetail
            if (! CollectionUtils.isEmpty(stakingProductRebateDetailList)) {
                // 保存派息计算结果
                productRebateDetailBatchMapper.insertList(stakingProductRebateDetailList);
            }
        } catch (Exception ex) {
            log.error("staking insertOrUpdateAssetSnapshotAndInsertRebateDetail error:{}, InsertUser:{}", ex, assetSnapshotIds);
            throw ex;
        }
    }

    /**
     * 理财产品：锁仓
     * 更新派息记录状态为'已派息'
     * 更新锁仓申购订单的最后派息日期
     * 如果是还本金则更新订单状态为已赎回
     *
     * @param productRebateDetail 理财项目派息记录
     */
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void updateLockPositionRebateDetailStatus(StakingProductRebateDetail productRebateDetail) {

        Long currentTime = System.currentTimeMillis();

        try {
            // 1. 修改派息记录状态为"已转账"
            productRebateDetailMapper.updateStakingRebateDetailStatus(productRebateDetail.getOrgId(), productRebateDetail.getId()
                    , StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_SUCCESS_VALUE
                    , currentTime);

            stakingProductOrderMapper.updateStakingProductOrderLastInterestDate(productRebateDetail.getOrgId(), productRebateDetail.getOrderId()
                    , StakingUtils.getDayDate(currentTime)
                    , currentTime);

            // 2. 如果是本金转账记录，则修改该订单状态为已赎回
            if (productRebateDetail.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_PRINCIPAL_VALUE) {
                // 修改理财申购订单的状态为已赎回
                stakingProductOrderMapper.updateStakingProductRedeemStatusAndDate(productRebateDetail.getOrgId(), productRebateDetail.getOrderId()
                        , StakingProductOrderStatus.STPD_ORDER_STATUS_REDEEMED_VALUE
                        , currentTime
                        , currentTime);

                // 3. 理财还本流水记录
                stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                        .id(productRebateDetail.getTransferId())
                        .orgId(productRebateDetail.getOrgId())
                        .userId(productRebateDetail.getUserId())
                        .accountId(productRebateDetail.getUserAccountId())
                        .productId(productRebateDetail.getProductId())
                        .productType(productRebateDetail.getProductType())
                        .tokenId(productRebateDetail.getTokenId())
                        .type(StakingProductJourType.STPD_JOUR_TYPE_REDEEM_VALUE)
                        .amount(productRebateDetail.getRebateAmount())
                        .transferId(productRebateDetail.getTransferId())
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build());
            } else {
                // 3. 理财付息流水记录
                stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                        .id(productRebateDetail.getTransferId())
                        .orgId(productRebateDetail.getOrgId())
                        .userId(productRebateDetail.getUserId())
                        .accountId(productRebateDetail.getUserAccountId())
                        .productId(productRebateDetail.getProductId())
                        .productType(productRebateDetail.getProductType())
                        .tokenId(productRebateDetail.getTokenId())
                        .type(StakingProductJourType.STPD_JOUR_TYPE_INTEREST_VALUE)
                        .amount(productRebateDetail.getRebateAmount())
                        .transferId(productRebateDetail.getTransferId())
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build());
            }
        } catch (Exception ex) {
            log.error("staking updateLockPositionRebateDetailStatus error:{}, productRebateDetail:{}", ex, productRebateDetail);
            throw ex;
        }
    }

    /**
     * 更新申购订单状态为申购成功
     *
     * @param productOrder 申购订单
     * @param currentTime  时间戳
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void updateStakingProductLockPositionOrderStatus(StakingProductOrder productOrder, Integer orderStatus, Long currentTime) {
        try {
            // 1.update order status success
            int rtnRows = stakingProductOrderMapper.updateStakingProductOrderSuccess(productOrder.getOrgId()
                    , productOrder.getId(), orderStatus, currentTime);
            if (rtnRows != 0) {
                // 2. 理财流水记录
                stakingProductJourMapper.insertSelective(StakingProductJour.builder()
                        .id(productOrder.getTransferId())
                        .orgId(productOrder.getOrgId())
                        .userId(productOrder.getUserId())
                        .accountId(productOrder.getAccountId())
                        .productId(productOrder.getProductId())
                        .productType(productOrder.getProductType())
                        .tokenId(productOrder.getTokenId())
                        .type(StakingProductJourType.STPD_JOUR_TYPE_SUBSCRIBE_VALUE)
                        .amount(productOrder.getPayAmount())
                        .transferId(productOrder.getTransferId())
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build());
            }
        } catch (Exception ex) {
            log.error("staking updateStakingProductLockPositionOrderStatus error:{}, Message:{}", ex, productOrder.toString());
            throw ex;
        }
    }

    /**
     * 申购验证
     *
     * @param stakingSchema  申购对象
     * @param stakingProduct 理财产品
     * @return 校验状态码
     */
    public Integer validation(StakingSchema stakingSchema, StakingProduct stakingProduct) {

        int rtnCode = BrokerErrorCode.SUCCESS.code();

        SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(stakingSchema.getOrgId(), BaseConfigConstants.FROZEN_USER_BONUS_TRADE_GROUP
                , stakingSchema.getUserId() + "");
        if(switchStatus.isOpen()){
            log.warn("staking purchase frozen:orgId{}-userId{}", stakingSchema.getOrgId(), stakingSchema.getUserId());
            return BrokerErrorCode.USER_STATUS_FORBIDDEN.code();
        }

        // 1. 参数校验：申购手数是否正确
        if (stakingSchema.getLots() <= 0) {
            return BrokerErrorCode.PARAM_ERROR.code();
        }

        // 2. 产品信息校验
        int productValidation = validationStakingProductInfo(stakingProduct);
        if (productValidation != rtnCode) {
            return productValidation;
        }

        // 3. 验证申购条件
        int validationConditionRtn = validationSubscribeCondition(stakingSchema);
        if (validationConditionRtn != rtnCode) {
            return validationConditionRtn;
        }

        // 4. 校验理财产品发售额度及用户申购额度
        rtnCode = validationSubscribeLimit(stakingSchema, stakingProduct);

        return rtnCode;
    }

    /**
     * 检查派息账户余额是否充足
     *
     * @param transferEvent 转账事件
     * @return 40001=参数错误 31018=账号不存在 33001=账户余额不足
     */
    public Integer checkDividendAccount(StakingTransferEvent transferEvent) {
        // 1. get product
        StakingProduct stakingProduct = getStakingProduct(transferEvent.getOrgId(), transferEvent.getProductId());
        transferEvent.setProductType(stakingProduct.getType());

        // 2. get dividend amount
        List<StakingProductRebateDetailTotal> listTotal = productRebateDetailMapper.listStakingProductRebateDetailTotalById(transferEvent);
        if (CollectionUtils.isEmpty(listTotal)) {
            log.warn("[staking] rebate detail data is null,{}", transferEvent.toString());
            stakingProductRebateMapper.updateStatus(transferEvent.getOrgId(), transferEvent.getRebateId()
                    , StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS_VALUE, System.currentTimeMillis());
        } else {
            // 3. 利息账户校验
            Optional<StakingProductRebateDetailTotal> optionalInterest = listTotal.stream().filter(x -> x.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_INTEREST_VALUE).findFirst();
            if (optionalInterest.isPresent()) {
                StakingProductRebateDetailTotal interestTotal = optionalInterest.get();
                if (interestTotal.getAmount().compareTo(BigDecimal.ZERO) > 0) {
                    Balance dividendBalance = accountService.queryTokenBalance(stakingProduct.getOrgId()
                            , stakingProduct.getDividendAccountId(), interestTotal.getTokenId());
                    if (dividendBalance != null) {
                        if (interestTotal.getAmount().compareTo(new BigDecimal(dividendBalance.getFree())) > 0) {
                            return BrokerErrorCode.INSUFFICIENT_BALANCE.code();
                        }
                    } else {
                        return BrokerErrorCode.ACCOUNT_NOT_EXIST.code();
                    }
                }
            }
            // 锁仓不校验本金账户
            if (stakingProduct.getType() != StakingProductType.LOCK_POSITION_VALUE) {
                // 3. 本金账户校验
                Optional<StakingProductRebateDetailTotal> principalOptional = listTotal.stream().filter(x -> x.getRebateType() == StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_PRINCIPAL_VALUE).findFirst();
                if (principalOptional.isPresent()) {
                    StakingProductRebateDetailTotal principalTotal = principalOptional.get();
                    if (principalTotal.getAmount().compareTo(BigDecimal.ZERO) > 0) {
                        Balance principalBalance = accountService.queryTokenBalance(stakingProduct.getOrgId()
                                , stakingProduct.getPrincipalAccountId(), principalTotal.getTokenId());
                        if (principalBalance != null) {
                            if (principalTotal.getAmount().compareTo(new BigDecimal(principalBalance.getFree())) > 0) {
                                return BrokerErrorCode.INSUFFICIENT_BALANCE.code();
                            }
                        } else {
                            return BrokerErrorCode.ACCOUNT_NOT_EXIST.code();
                        }
                    }
                }
            }
        }
        return BrokerErrorCode.SUCCESS.code();
    }

    /**
     * 获取产品信息
     *
     * @param orgId     机构ID
     * @param productId 理财产品ID
     * @return 理财产品信息
     */
    public StakingProduct getStakingProduct(Long orgId, Long productId) {
        StakingProduct stakingProduct;
        try {
            stakingProduct = STAKING_PRODUCT_CACHE.get(productId, () ->
                    stakingProductMapper.selectOne(StakingProduct.builder().orgId(orgId)
                            .id(productId).build()));
        } catch (Exception e) {
            stakingProduct = null;
        }
        return stakingProduct;
    }

    /**
     * 理财产品信息验证
     *
     * @param stakingProduct 理财产品
     * @return 验证结果状态码
     */
    private Integer validationStakingProductInfo(StakingProduct stakingProduct) {
        long currentTime = System.currentTimeMillis();

        // 验证理财产品信息
        if (stakingProduct == null || stakingProduct.getIsShow() == 0) {
            return BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code();
        }

        // 申购是否已经开始
        if (stakingProduct.getSubscribeStartDate() > currentTime) {
            return BrokerErrorCode.FINANCE_NOT_OPEN.code();
        }

        if(stakingProduct.getType() == StakingProductType.FI_CURRENT_VALUE){
            // 活期理财提前两天停止申购
            long endSubscribeDate = stakingProduct.getSubscribeEndDate() - 1000 * 60 * 60 * 24 * 2;
            if (endSubscribeDate < currentTime) {
                return BrokerErrorCode.FINANCE_PRODUCT_STOP_PURCHASE.code();
            }
        } else {
            // 申购是否已经结束（活期项目则表示项目结束）
            if (stakingProduct.getSubscribeEndDate() < currentTime) {
                return BrokerErrorCode.FINANCE_PRODUCT_STOP_PURCHASE.code();
            }
        }
        return BrokerErrorCode.SUCCESS.code();
    }

    /**
     * 通过Redis Cache校验个人可申购上限和产品总发行额度是否超限
     *
     * @param stakingSchema  申购对象
     * @param stakingProduct 理财产品对象
     * @return 校验结果状态码
     */
    private Integer validationSubscribeLimit(StakingSchema stakingSchema, StakingProduct stakingProduct) {

        int surplusLots = stakingProduct.getUpLimitLots() - stakingProduct.getSoldLots();

        // 校验申购金额是否按照申购梯度
        if (stakingSchema.getAmount().subtract(stakingProduct.getPerLotAmount()).remainder(stakingProduct.getPerLotAmount()).compareTo(BigDecimal.ZERO) != 0) {
            return BrokerErrorCode.FINANCE_ILLEGAL_PURCHASE_AMOUNT.code();
        }

        // 校验剩余额度
        if (surplusLots <= 0) {
            return BrokerErrorCode.FINANCE_INSUFFICIENT_BALANCE.code();
        }
        // 判断剩余额度是否足够
        if (stakingSchema.getLots() > surplusLots) {
            return BrokerErrorCode.FINANCE_TOTAL_LIMIT_OUT.code();
        }

        // 用户最低申购手数
        if (stakingSchema.getLots().compareTo(stakingProduct.getPerUsrLowLots()) < 0) {
            return BrokerErrorCode.FINANCE_PURCHASE_AMOUNT_TOO_SMALL.code();
        }

        // get user accountId
        Long accountId = accountService.getAccountId(stakingProduct.getOrgId(), stakingSchema.getUserId());
        stakingSchema.setUserAccountId(accountId);

        if (accountId.equals(stakingProduct.getDividendAccountId())
                || accountId.equals(stakingProduct.getPrincipalAccountId())) {
            return BrokerErrorCode.STAKING_SUBSCRIBE_ACCOUNT_CONFLICT.code();
        }

        BigDecimal userUsedAmount = BigDecimal.ZERO;
        // 锁仓产品通过申购记录合计
        if(stakingProduct.getType() == StakingProductType.LOCK_POSITION_VALUE){
            BigDecimal userSubscribedAmount = stakingProductOrderMapper.sumOrderAmountByUserIdAndProduct(stakingProduct.getOrgId()
                    , stakingProduct.getId(), stakingSchema.getUserId());
            if(userSubscribedAmount != null){
                userUsedAmount = userSubscribedAmount;
            }

        } else {
            // 校验用户已申购额度+当前申购额度是否超出个人允许申购上限
            StakingAsset stakingAsset = stakingAssetMapper.selectOne(StakingAsset.builder()
                    .orgId(stakingProduct.getOrgId())
                    .userId(stakingSchema.getUserId())
                    .productId(stakingProduct.getId())
                    .build());
            if(stakingAsset != null) {
                userUsedAmount = stakingAsset.getCurrentAmount();
            }
        }

        // 如果用户已持有+当前申购的金额大于大于产品允许的个人申购上限，则提示超出上限
        BigDecimal userCalcTotalAmount = userUsedAmount.add(stakingSchema.getAmount());
        BigDecimal productAllowPerUserTotalAmount = stakingProduct.getPerLotAmount().multiply(BigDecimal.valueOf(stakingProduct.getPerUsrUpLots()));
        if (userCalcTotalAmount.compareTo(productAllowPerUserTotalAmount) > 0) {
            return BrokerErrorCode.FINANCE_USER_LIMIT_OUT.code();
        }

        Balance balance = accountService.queryTokenBalance(stakingProduct.getOrgId(), stakingSchema.getUserAccountId(), stakingProduct.getTokenId());
        if (balance == null) {
            return BrokerErrorCode.GRPC_SERVER_TIMEOUT.code();
        }

        if (stakingSchema.getAmount().compareTo(new BigDecimal(balance.getFree())) > 0) {
            return BrokerErrorCode.FINANCE_PURCHASE_INSUFFICIENT_BALANCE.code();
        }

        return BrokerErrorCode.SUCCESS.code();
    }

    /**
     * 验证用户申购条件
     *
     * @param stakingSchema
     * @return
     */
    private Integer validationSubscribeCondition(StakingSchema stakingSchema) {
        int rtnCode = BrokerErrorCode.SUCCESS.code();
        try {
            businessLimitService.verifyLimit(stakingSchema.getOrgId(), stakingSchema.getUserId(), stakingSchema.getProductId(), EnumBusinessLimit.STAKING_PRODUCT);
        } catch (BrokerException ex) {
            rtnCode = ex.code();
        } catch (Exception ex) {
            rtnCode = BrokerErrorCode.SYSTEM_ERROR.code();
        }
        return rtnCode;
    }
}
