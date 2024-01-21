package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.model.staking.*;

import java.math.BigDecimal;
import java.util.Calendar;

/**
 * 理财工具类，转换proto协议对象
 */
public class StakingUtils {

    public final static int SCALE_NUM = 8;

    public static String decimalToString(BigDecimal value) {
        return value.stripTrailingZeros().toPlainString();
    }

    public static String decimalScaleToString(BigDecimal value) {
        return value.setScale(SCALE_NUM, BigDecimal.ROUND_DOWN).stripTrailingZeros().toPlainString();
    }

    public static BigDecimal decimalScale(BigDecimal value, int scale) {
        return value.setScale(scale, BigDecimal.ROUND_DOWN);
    }

    public static StakingProductLocalInfo convertProductLocalInfo(StakingProductLocal local) {

        return StakingProductLocalInfo.newBuilder()
                .setId(local.getId())
                .setProductId(local.getProductId())
                .setLang(local.getLanguage())
                .setProductDetails(local.getDetails())
                .setProductName(local.getProductName())
                .setProtocolUrl(local.getProtocolUrl())
                .setStatus(local.getStatus())
                .build();
    }

    public static StakingProductRebateInfo convertProductRebateInfo(StakingProductRebate rebate, BasicService basicService) {

        return StakingProductRebateInfo.newBuilder()
                .setId(rebate.getId())
                .setOrgId(rebate.getOrgId())
                .setProductId(rebate.getProductId())
                .setTokenId(rebate.getTokenId())
                .setRebateDate(rebate.getRebateDate())
                .setRebateRate(rebate.getRebateRate().stripTrailingZeros().toPlainString())
                .setRebateAmount(decimalScaleToString(rebate.getRebateAmount()))
                .setStatus(rebate.getStatus())
                .setRebateCalcWay(rebate.getRebateCalcWay())
                .setTokenName(basicService.getTokenName(rebate.getOrgId(),rebate.getTokenId()))
                .setCreatedAt(rebate.getCreatedAt())
                .setUpdatedAt(rebate.getUpdatedAt())
                .build();

    }

    public static StakingProductRebateDetailInfo convertProductRebateDetailInfo(StakingProductRebateDetail detail, String tokenName) {
        return StakingProductRebateDetailInfo.newBuilder()
                .setId(detail.getId())
                .setOrgId(detail.getOrgId())
                .setProductRebateId(detail.getProductRebateId())
                .setProductId(detail.getProductId())
                .setTokenId(detail.getTokenId())
                .setTokenName(tokenName)
                .setOrderId(detail.getOrderId())
                .setUserId(detail.getUserId())
                .setUserAccountId(detail.getUserAccountId())
                .setOriginAccountId(detail.getOriginAccountId())
                .setTransferId(detail.getTransferId())
                .setRebateType(detail.getRebateType())
                .setRebateAmount(decimalScaleToString(detail.getRebateAmount()))
                .setStatus(detail.getStatus())
                .setCreatedAt(detail.getCreatedAt())
                .setUpdatedAt(detail.getUpdatedAt())
                .build();
    }

    public static StakingProductOrderInfo convertProductOrderInfo(StakingProductOrder order, String tokenName) {
        return StakingProductOrderInfo.newBuilder()
                .setId(order.getId())
                .setOrgId(order.getOrgId())
                .setUserId(order.getUserId())
                .setAccountId(order.getAccountId())
                .setProductId(order.getProductId())
                .setProductType(order.getProductType())
                .setTransferId(order.getTransferId())
                .setPayLots(order.getPayLots())
                .setPayAmount(order.getPayAmount().stripTrailingZeros().toPlainString())
                .setTokenId(order.getTokenId())
                .setTokenName(tokenName)
                .setTakeEffectDate(order.getTakeEffectDate())
                .setRedemptionDate(order.getRedemptionDate())
                .setCanAutoRenew(order.getCanAutoRenew())
                .setStatus(order.getStatus())
                .setCreatedAt(order.getCreatedAt())
                .setUpdatedAt(order.getUpdatedAt())
                .build();
    }


    public static StakingPoolLocalInfo convertStakingLocalInfo(StakingPoolLocal local) {

        return StakingPoolLocalInfo.newBuilder()
                .setId(local.getId())
                .setOrgId(local.getOrgId())
                .setStakingId(local.getStakingId())
                .setLanguage(local.getLanguage())
                .setDetails(local.getDetails())
                .setStakingName(local.getStakingName())
                .setCreatedAt(local.getCreatedAt())
                .setUpdatedAt(local.getUpdatedAt())
                .build();
    }

    public static StakingPoolRebateInfo convertStakingRebateInfo(StakingPoolRebate rebate) {

        return StakingPoolRebateInfo.newBuilder()
                .setId(rebate.getId())
                .setStakingId(rebate.getStakingId())
                .setStakingType(rebate.getStakingType())
                .setTokenId(rebate.getTokenId())
                .setRebateDate(rebate.getRebateDate())
                .setRebateCoinAmount(rebate.getRebateCoinAmount().stripTrailingZeros().toPlainString())
                .setStatus(rebate.getStatus())
                .setApr(rebate.getApr().stripTrailingZeros().toPlainString())
                .setCreatedAt(rebate.getCreatedAt())
                .setUpdatedAt(rebate.getUpdatedAt())
                .build();

    }

    /**
     * 获取yyyyMMdd的long
     * @param currentTime
     * @param spanDays
     * @return
     */
    public static Long getDayDate(Long currentTime, Integer spanDays) {
        Calendar calendar=Calendar.getInstance();
        calendar.setTimeInMillis(currentTime);
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        if(spanDays != 0){
            calendar.add(Calendar.DAY_OF_MONTH, spanDays);
        }
        return calendar.getTimeInMillis();
    }

    public static Long getDayDate(Long currentTime) {
        return getDayDate(currentTime, 0);
    }

    public static StakingProductSubscribeLimitInfo convertProductSubscribeLimitInfo(StakingProductSubscribeLimit limit) {
        return StakingProductSubscribeLimitInfo.newBuilder()
                .setId(limit.getId())
                .setOrgId(limit.getOrgId())
                .setProductId(limit.getProductId())
                .setVerifyKyc(limit.getVerifyKyc())
                .setVerifyBindPhone(limit.getVerifyBindPhone())
                .setVerifyBalance(limit.getVerifyBalance())
                .setVerifyAvgBalance(limit.getVerifyAvgBalance())
                .setBalanceRuleJson(limit.getBalanceRuleJson())
                .setLevelLimit(limit.getLevelLimit())
                .setCreatedTime(limit.getCreatedTime())
                .setUpdatedTime(limit.getUpdatedTime())
                .build();
    }
}
