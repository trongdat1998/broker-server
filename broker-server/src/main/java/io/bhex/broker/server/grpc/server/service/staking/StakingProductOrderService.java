package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.domain.staking.StakingConstant;
import io.bhex.broker.server.domain.staking.StakingPoolRebateDetailStatus;
import io.bhex.broker.server.domain.staking.StakingPoolRebateStatus;
import io.bhex.broker.server.domain.staking.StakingSchema;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.po.StakingRedeemResponseDTO;
import io.bhex.broker.server.grpc.server.service.po.StakingSubscribeResponseDTO;
import io.bhex.broker.server.message.StakingTransferMessage;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 理财产品申购、赎回服务
 *
 * @author songxd
 * @date 2020-07-29
 */
@Slf4j
@Service
public class StakingProductOrderService {

    @Resource
    private StakingPoolRebateMapper stakingRebateMapper;

    @Resource
    private StakingPoolRebateDetailMapper stakingRebateDetailMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private StakingProductTimeStrategy stakingProductTimeStrategy;

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    @Resource
    private StakingProductLockedPositionStrategy stakingProductLockedPositionStrategy;

    @Resource
    private StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    private StakingProductRebateMapper stakingProductRebateMapper;

    @Resource
    private StakingAssetMapper stakingAssetMapper;

    @Resource
    private StakingProductStrategyService productStrategyService;

    @Resource
    private ISequenceGenerator<Long> sequenceGenerator;

    @Autowired
    private ApplicationContext applicationContext;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private BasicService basicService;

    @Resource
    private StakingProductStrategyService stakingProductStrategyService;

    /**
     * 理财申购:定期、活期、 锁仓
     *
     * @param header
     * @param tokenId
     * @param productId 理财商品ID
     * @param lots      申购手数
     * @return 返回申购流水号
     */
    public StakingSubscribeResponseDTO subscribeProduct(Header header, String tokenId, Long productId
            , Integer lots, BigDecimal amounts , StakingProductType stakingType) {

        StakingSchema.StakingSchemaBuilder schemaBuilder = StakingSchema.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .productId(productId)
                .lots(lots)
                .amount(amounts);

        if (stakingType == StakingProductType.FI_TIME) {
            Map<String, String> map = new HashMap<>(1);
            map.put(StakingConstant.CAN_AUTO_RENEW, "0");
            schemaBuilder.attrProperty(map);
            return stakingProductTimeStrategy.subscribe(schemaBuilder.build());
        } else if (stakingType == StakingProductType.FI_CURRENT) {
            return stakingProductCurrentStrategy.subscribe(schemaBuilder.build());
        } else if (stakingType == StakingProductType.LOCK_POSITION) {
            return stakingProductLockedPositionStrategy.subscribe(schemaBuilder.build());
        } else {
            return null;
        }
    }

    /**
     * 根据申请流水号从缓存中获取申购结果
     *
     * @param header
     * @param productId
     * @param transferId
     * @return
     */
    public Integer getSubscribeResult(Header header, Long productId, Long transferId) {

        int rtnCode = BrokerErrorCode.STAKING_PROCESSING.code();

        // 验证transferId是否存在
        String strTransferId = redisTemplate.opsForValue().get(String.format(StakingConstant.STAKING_TRANSFER_CACHE_KEY
                , header.getOrgId(), header.getUserId(), productId, transferId));

        if (StringUtils.isEmpty(strTransferId) || !String.valueOf(transferId).equals(strTransferId)) {
            return BrokerErrorCode.STAKING_TRANSFER_ID_ILLEGAL.code();
        }

        String subscribeStatusKey = String.format(StakingConstant.SUBSCRIBE_STATUS_CODE_PREFIX, header.getOrgId()
                , header.getUserId()
                , productId
                , transferId);

        String statusCode = redisTemplate.opsForValue().get(subscribeStatusKey);

        if (StringUtils.isNotEmpty(statusCode)) {
            rtnCode = Integer.parseInt(statusCode);
        } else {
            StakingProductOrder productOrder = stakingProductOrderMapper.selectOne(StakingProductOrder.builder()
                    .orgId(header.getOrgId())
                    .userId(header.getUserId())
                    .productId(productId)
                    .transferId(transferId).build());
            if (productOrder != null) {
                if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_SUCCESS_VALUE) {
                    rtnCode = BrokerErrorCode.SUCCESS.code();
                } else if (productOrder.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE) {
                    rtnCode = BrokerErrorCode.FINANCE_PURCHASE_ERROR.code();
                }
            }
        }
        return rtnCode;
    }

    /**
     * 后台触发：派息指令
     *
     * @param orgId           机构ID
     * @param productId       理财产品ID
     * @param productRebateId 理财产品派息设置ID
     * @return 转账结果状态码
     */
    public Integer dividendTransfer(Long orgId, Long productId, Long productRebateId) {
        Integer rtnCode;
        // send message

        String lockKey = String.format(StakingConstant.STAKING_PRODUCT_DIVIDEND_LOCK_KEY
                , orgId, productId, productRebateId);

        boolean rtn = RedisLockUtils.tryLock(redisTemplate, lockKey, StakingConstant.STAKING_PRODUCT_LOCK_EXPIRE_SECOND);
        if (!rtn) {
            return BrokerErrorCode.REPEATED_SUBMIT_REQUEST.code();
        }

        try {

            StakingProductRebate stakingProductRebate = stakingProductRebateMapper.selectOne(StakingProductRebate.builder()
                    .id(productRebateId).orgId(orgId).productId(productId).build());
            if (stakingProductRebate == null) {
                return BrokerErrorCode.PARAM_ERROR.code();
            } else if (stakingProductRebate.getStatus() == StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS_VALUE) {
                return BrokerErrorCode.SUCCESS.code();
            } else if (stakingProductRebate.getStatus() == StakingProductRebateStatus.STPD_REBATE_STATUS_CANCELED_VALUE) {
                return BrokerErrorCode.CANCEL_ORDER_ARCHIVED.code();
            }

            StakingTransferEvent stakingTransferEvent = StakingTransferEvent.builder()
                    .orgId(orgId)
                    .productId(productId)
                    .rebateId(productRebateId)
                    .rebateType(stakingProductRebate.getType())
                    .build();

            // 检查转账账户及余额是否足够
            rtnCode = productStrategyService.checkDividendAccount(stakingTransferEvent);
            if (rtnCode == BrokerErrorCode.SUCCESS.code()) {
                // 异步转账更新资产
                CompletableFuture.runAsync(() ->
                        applicationContext.publishEvent(stakingTransferEvent), taskExecutor);
            }
        } catch (Exception e) {
            rtnCode = BrokerErrorCode.SYSTEM_ERROR.code();
            log.error("dividendTransfer error: message => {},{},{}", orgId, productId, productRebateId, e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }
        return rtnCode;
    }

    /**
     * 取消派息转账
     *
     * @param orgId
     * @param productId
     * @param rebateId
     * @return
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public Boolean cancelDividend(Long orgId, Long productId, Long rebateId) {

        long currentTime = System.currentTimeMillis();
        boolean rtn = false;

        try {
            int rebateUpdate = stakingRebateMapper.updateStatus(rebateId, StakingPoolRebateStatus.CANCELED.getStatus(), currentTime);
            int rebateDetailsUpdate = stakingRebateDetailMapper.batchUpdateStatusByStakingRebateId(orgId, productId, rebateId
                    , StakingPoolRebateDetailStatus.CANCELED.getStatus()
                    , currentTime);
            if (rebateUpdate > 0 && rebateDetailsUpdate > 0) {
                rtn = true;
            }
        } catch (Exception e) {
            log.error("cancel staking rebate for orgid:{} productId:{} rebateid:{} Error: {}.", orgId, productId, rebateId, e);
            throw e;
        }

        return rtn;
    }

    /**
     * 计算利息:后台发起
     *
     * @param orgId     机构ID
     * @param productId 理财产品ID
     * @param rebateId  理财产品派息设置ID
     * @return
     */
    public StakingCalcInterestReply calcInterest(Long orgId, Long productId, Long rebateId) {

        StakingCalcInterestReply.Builder builder = StakingCalcInterestReply.newBuilder();

        // lock
        String lockKey = String.format(StakingConstant.STAKING_RE_CALC_INTEREST_LOCK, orgId, productId, rebateId);
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, StakingConstant.STAKING_RE_CALC_INTEREST_LOCK_EXPIRE);
        if (!lock) {
            return builder.setCode(BrokerErrorCode.REQUEST_TOO_FAST.code()).setMessage(BrokerErrorCode.REQUEST_TOO_FAST.msg()).build();
        }

        StakingProduct stakingProduct = stakingProductStrategyService.getStakingProduct(orgId, productId);
        if (stakingProduct == null) {
            builder.setCode(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code()).setMessage(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.msg());
            return builder.build();
        }

        StakingProductRebate productRebate = stakingProductRebateMapper.selectOne(StakingProductRebate.builder()
                .orgId(orgId)
                .productId(productId)
                .id(rebateId)
                .build());
        if (productRebate == null) {
            builder.setCode(BrokerErrorCode.PARAM_ERROR.code()).setMessage(BrokerErrorCode.PARAM_ERROR.msg());
            return builder.build();
        }

        if (productRebate.getRebateRate().compareTo(BigDecimal.ZERO) <= 0
                && productRebate.getRebateAmount().compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("staking product rebate not set rebate rate and rebate amount: org:{} product:{} rebate:{}"
                    , productRebate.getOrgId(), productRebate.getProductId(), productRebate.getId());
            builder.setCode(BrokerErrorCode.PARAM_ERROR.code()).setMessage(BrokerErrorCode.PARAM_ERROR.msg());
            return builder.build();
        }

        switch (stakingProduct.getType()) {
            case StakingProductType.FI_TIME_VALUE:
                stakingProductTimeStrategy.reCalcInterest(productRebate, stakingProduct, System.currentTimeMillis());
                break;
            case StakingProductType.LOCK_POSITION_VALUE:
                stakingProductLockedPositionStrategy.reCalcInterest(productRebate, stakingProduct, System.currentTimeMillis());
                break;
            case StakingProductType.FI_CURRENT_VALUE:
                stakingProductCurrentStrategy.reCalcInterest(productRebate, stakingProduct, System.currentTimeMillis());
                break;
            default:
                break;
        }

        return builder.setCode(BrokerErrorCode.SUCCESS.code()).setMessage(BrokerErrorCode.SUCCESS.msg()).build();
    }

    /**
     * 理财赎回
     *
     * @param header    orgId and userId
     * @param productId 理财产品ID
     * @param orderId   定期项目以orderId，活期项目则为assetId
     * @param amount    赎回金额
     * @return
     */
    public StakingRedeemResponseDTO redeemAmount(Header header, Long productId, Long orderId
            , BigDecimal amount, StakingProductType stakingProductType) {
        if (stakingProductType == StakingProductType.FI_CURRENT) {
            Long transferId = sequenceGenerator.getLong();
            // 原币多多是直接以产品ID赎回，所以这里取下资产ID
            if(orderId == null || orderId == 0){
                StakingAsset stakingAsset = stakingAssetMapper.selectOne(StakingAsset.builder()
                        .orgId(header.getOrgId())
                        .userId(header.getUserId())
                        .productId(productId)
                        .build());
                if(stakingAsset == null){
                    return StakingRedeemResponseDTO.builder().code(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code()).build();
                } else{
                    orderId = stakingAsset.getId();
                }
            }
            int rtnCode = stakingProductCurrentStrategy.processRedeemRequest(header, productId, orderId, amount, transferId);
            return StakingRedeemResponseDTO.builder().code(rtnCode).transferId(transferId).build();
        } else {
            return StakingRedeemResponseDTO.builder().code(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code()).build();
        }
    }

    /**
     * 理财赎回结果
     *
     * @param header
     * @param productId
     * @param transferId
     * @return
     */
    public Integer redeemResult(Header header, Long productId, Long transferId) {
        return 0;
    }

    /**
     * 获取单个申购订单信息
     *
     * @param header
     * @param productId
     * @param transferId
     * @param orderId
     * @return
     */
    public GetStakingProductOrderResponse getStakingProductOrder(Header header, Long productId, Long transferId, Long orderId) {

        GetStakingProductOrderResponse.Builder builder = GetStakingProductOrderResponse.newBuilder();

        StakingProductOrder stakingProductOrder;
        if (orderId.compareTo(0L) > 0) {
            stakingProductOrder = stakingProductOrderMapper.selectOne(StakingProductOrder.builder().id(orderId).orgId(header.getOrgId()).build());
        } else {
            stakingProductOrder = stakingProductOrderMapper.getByTransferId(header.getOrgId(), header.getUserId(), productId, transferId);
        }
        builder.setStakingProductOrderInfo(StakingUtils.convertProductOrderInfo(stakingProductOrder
                , basicService.getTokenName(stakingProductOrder.getOrgId(), stakingProductOrder.getTokenId())));

        return builder.build();
    }

    /**
     * 更新持仓派息记录的状态
     *
     * @param stakingRebateDetail
     */
    private void updateStakingPoolRebateDetailStatus(StakingPoolRebateDetail stakingRebateDetail) {
        stakingRebateDetailMapper.updateStatus(stakingRebateDetail.getId()
                , System.currentTimeMillis()
                , StakingPoolRebateDetailStatus.SUCCESS.getStatus());
    }

    /**
     * 理财：持仓生息派息转账操作
     *
     * @param stakingTransferMessage
     */
    @Deprecated
    private void stakingPoolHoldPostionRebateTransfer(StakingTransferMessage stakingTransferMessage) {
        Boolean runStakingStatus = true;
        Long currentTime = System.currentTimeMillis();
        List<StakingPoolRebateDetail> stakingRebateDetailList;

        while (runStakingStatus) {
            // select
            stakingRebateDetailList = stakingRebateDetailMapper.getWatingTransferList(stakingTransferMessage.getOrgId()
                    , stakingTransferMessage.getProductId()
                    , stakingTransferMessage.getRebateId());
            if (CollectionUtils.isEmpty(stakingRebateDetailList)) {
                runStakingStatus = false;
                // 更新该派息设置已派息完成
                stakingRebateMapper.updateStatus(stakingTransferMessage.getRebateId()
                        , StakingPoolRebateStatus.SUCCESS.getStatus()
                        , currentTime);

            } else {
                // 逐一转账
                for (StakingPoolRebateDetail stakingRebateDetail : stakingRebateDetailList) {
                    // 转账成功，更新派息记录状态
                    // TODO dividendTransfer(stakingRebateDetail);
                    Boolean transferResult = false;
                    if (transferResult) {
                        // update status
                        updateStakingPoolRebateDetailStatus(stakingRebateDetail);
                    }
                }
            }
        }
    }


}
