package io.bhex.broker.server.util;

import io.bhex.base.proto.Decimal;
import io.bhex.broker.common.util.ExtraConfigUtil;
import io.bhex.broker.common.util.ExtraTagUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.stream.Collectors;

import io.bhex.base.account.NewPlanOrderRequest;
import io.bhex.base.account.OptionSettlements;
import io.bhex.base.account.PlanOrderCloseType;
import io.bhex.base.account.PlanOrderQuotePriceType;
import io.bhex.base.account.PlanOrderTypeEnum;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenCategory;
import io.bhex.base.token.TokenFuturesInfo;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.basic.FuturesRiskLimit;
import io.bhex.broker.grpc.basic.Underlying;
import io.bhex.broker.grpc.order.CreateFuturesOrderRequest;
import io.bhex.broker.grpc.order.FuturesOrderSetting;
import io.bhex.broker.grpc.order.FuturesOrderSide;
import io.bhex.broker.grpc.order.FuturesPriceType;
import io.bhex.broker.grpc.order.FuturesSettlement;
import io.bhex.broker.grpc.order.InsuranceFund;
import io.bhex.broker.grpc.order.OrderSide;
import io.bhex.broker.grpc.order.PlanOrder;
import io.bhex.broker.grpc.order.ProfitLossResult;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.model.Symbol;
import lombok.extern.slf4j.Slf4j;

import static io.bhex.broker.server.domain.BrokerServerConstants.FUTURES_LEVERAGE_PRECISION;

@Slf4j
public class FuturesUtil {

    public static final String USDT = "USDT";

    public static final String BTC = "BTC";

    public static final String LONG_VALUE = "1";

    public static final String SHORT_VALUE = "0";

    public static boolean isFutureCategory(List<Integer> categories) {
        return !CollectionUtils.isEmpty(categories)
                && categories.iterator().next() == TokenCategory.FUTURE_CATEGORY.getNumber();
    }

    public static io.bhex.broker.grpc.basic.Symbol toFuturesSymbol(io.bhex.broker.server.model.Symbol underlyingDetail, SymbolDetail symbolDetail,
                                                                   TokenFuturesInfo tokenFutures) {
        return io.bhex.broker.grpc.basic.Symbol.newBuilder()
                .setOrgId(underlyingDetail.getOrgId())
                .setExchangeId(symbolDetail.getExchangeId())
                .setSymbolId(symbolDetail.getSymbolId())
                .setSymbolName(symbolDetail.getSymbolName())
                .setBaseTokenId(symbolDetail.getBaseTokenId())
                .setBaseTokenName(symbolDetail.getBaseTokenId())
                .setQuoteTokenId(symbolDetail.getQuoteTokenId())
                .setQuoteTokenName(symbolDetail.getQuoteTokenId())
                .setBasePrecision(DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().toPlainString())
                .setQuotePrecision(DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().toPlainString())
                .setMinTradeQuantity(DecimalUtil.toBigDecimal(symbolDetail.getMinTradeQuantity()).stripTrailingZeros().toPlainString())
                .setMinTradeAmount(DecimalUtil.toBigDecimal(symbolDetail.getMinTradeAmount()).stripTrailingZeros().toPlainString())
                .setMinPricePrecision(DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().toPlainString())
                .setDigitMerge(symbolDetail.getDigitMergeList())
                .setCanTrade(symbolDetail.getAllowTrade()
                        && underlyingDetail.getAllowTrade() == BrokerServerConstants.ALLOW_STATUS.intValue()
                        && underlyingDetail.getStatus() == BrokerServerConstants.ONLINE_STATUS.intValue())
                .setBanSellStatus(underlyingDetail.getBanSellStatus() == 1)
                .setCustomOrder(underlyingDetail.getCustomOrder())
                .setIndexShow(underlyingDetail.getIndexShow() == 1)
                .setIndexShowOrder(underlyingDetail.getIndexShowOrder())
                .setCategory(underlyingDetail.getCategory())
                .setTokenFutures(toTokenFutures(tokenFutures, symbolDetail))
                .setIndexToken(symbolDetail.getIndexToken())
                .setIsReverse(symbolDetail.getIsReverse())
                .setDisplayIndexToken(symbolDetail.getDisplayIndexToken())
                .setIndexRecommendOrder(underlyingDetail.getIndexRecommendOrder())
                .setCustomLabelId(underlyingDetail.getLabelId())
                .setHideFromOpenapi(underlyingDetail.getHideFromOpenapi() == 1)
                .setForbidOpenapiTrade(underlyingDetail.getForbidOpenapiTrade() == 1)
                .putAllExtraTag(ExtraTagUtil.newInstance(underlyingDetail.getExtraTag()).map())
                .putAllExtraConfig(ExtraConfigUtil.newInstance(underlyingDetail.getExtraConfig()).map())
                .setIsAggragate(symbolDetail.getIsAggregate())
                .setIsTest(symbolDetail.getIsTest())
                .build();
    }


    private static io.bhex.broker.grpc.basic.TokenFuturesInfo toTokenFutures(TokenFuturesInfo t, SymbolDetail symbolDetail) {
        return io.bhex.broker.grpc.basic.TokenFuturesInfo.newBuilder()
                .setTokenId(t.getTokenId())
                .setDisplayTokenId(symbolDetail.getDisplayTokenId())
                .setDisplayUnderlyingId(symbolDetail.getDisplayUnderlyingId())
                .setMarginPrecision(DecimalUtil.toTrimString(symbolDetail.getMarginPrecision()))
                .setIssueDate(t.getIssueDate())
                .setSettlementDate(t.getSettlementDate())
                .setCurrency(t.getCurrency())
                .setCurrencyDisplay(t.getCurrencyDisplay())
                .setContractMultiplier(DecimalUtil.toTrimString(t.getContractMultiplier()))
                .setLimitDownInTradingHours(DecimalUtil.toTrimString(t.getLimitDownInTradingHours()))
                .setLimitUpInTradingHours(DecimalUtil.toTrimString(t.getLimitUpInTradingHours()))
                .setLimitDownOutTradingHours(DecimalUtil.toTrimString(t.getLimitDownOutTradingHours()))
                .setLimitUpOutTradingHours(DecimalUtil.toTrimString(t.getLimitUpOutTradingHours()))
                .setRiskLimitStep(t.getRiskLimitStep())
                .setCoinToken(t.getCoinToken())
                .addAllRiskLimits(toRiskLimits(t.getRiskLimitsList(), symbolDetail))
                .setMaxLeverage(DecimalUtil.toTrimString(t.getMaxLeverage()))
                .setLeverageRange(t.getLeverageRange())
                .setOverPriceRange(t.getOverPriceRange())
                .setMarketPriceRange(t.getMarketPriceRange())
                .build();
    }

    public static List<FuturesRiskLimit> toRiskLimits(List<io.bhex.base.proto.FuturesRiskLimit> riskLimits, SymbolDetail symbolDetail) {
        return riskLimits.stream()
                .map(riskLimit -> toRiskLimit(riskLimit, symbolDetail))
                .collect(Collectors.toList());
    }

    private static FuturesRiskLimit toRiskLimit(io.bhex.base.proto.FuturesRiskLimit t, SymbolDetail symbolDetail) {
        // 返回给前端的is_long需要取反（多仓转空仓，空仓转多仓）
        boolean displayIsLong = symbolDetail.getIsReverse() != t.getIsLong();

        return FuturesRiskLimit.newBuilder()
                .setRiskLimitId(t.getRiskLimitId())
                .setSymbolId(t.getSymbolId())
                .setRiskLimitAmount(DecimalUtil.toTrimString(t.getRiskLimitAmount()))
                .setMaintainMargin(DecimalUtil.toTrimString(t.getMaintainMargin()))
                .setInitialMargin(DecimalUtil.toTrimString(t.getInitialMargin()))
                .setIsLong(displayIsLong)
                .setCreatedAt(t.getCreatedAt())
                .setUpdatedAt(t.getUpdatedAt())
                .build();
    }

    public static Underlying toUnderlying(io.bhex.base.token.Underlying t) {
        return Underlying.newBuilder()
                .setUnderlyingId(t.getUnderlyingId())
                .setType(t.getType())
                .setParentUnderlyingId(t.getParentUnderlyingId())
                .setLevels(t.getLevels())
                .setTag(t.getTag())
                .build();
    }

    public static boolean validSymbolDetail(SymbolDetail symbolDetail) {
        if (symbolDetail == null || symbolDetail.getExchangeId() == 0 || symbolDetail.getSymbolId() == null) {
            return false;
        }
        return true;
    }


    public static FuturesOrderSetting toOrderSetting(io.bhex.base.account.FuturesOrderSetting orderSetting) {
        return FuturesOrderSetting.newBuilder()
                .setAccountId(orderSetting.getAccountId())
                .setIsPassiveOrder(orderSetting.getIsPassiveOrder())
                .setTimeInForce(orderSetting.getTimeInForce())
                .setIsConfirm(orderSetting.getIsConfirm())
                .build();
    }

    public static BigDecimal div(BigDecimal a, BigDecimal b) {
        if (b.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        return a.divide(b, 18, RoundingMode.DOWN);
    }

    public static FuturesSettlement buildSettlement(OptionSettlements optionSettlements, Symbol symbol, SymbolDetail symbolDetail) {
        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();
        return FuturesSettlement
                .newBuilder()
                .setAccountId(optionSettlements.getAccountId())
                .setSymbolName(optionSettlements.getSymbolName())
                .setSymbolId(optionSettlements.getTokenId())
                .setAvailable(DecimalUtil.toBigDecimal(optionSettlements.getAvailable()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAveragePrice(DecimalUtil.toBigDecimal(optionSettlements.getAveragePrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setChanged(DecimalUtil.toBigDecimal(optionSettlements.getChanged()).setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setChangedRate(DecimalUtil.toBigDecimal(optionSettlements.getChangedRate()).setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setCostPrice(DecimalUtil.toBigDecimal(optionSettlements.getCostPrice()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setMargin(DecimalUtil.toBigDecimal(optionSettlements.getMargin()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setMaxPayOff(DecimalUtil.toBigDecimal(optionSettlements.getMaxPayOff()).setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setSettlementId(optionSettlements.getSettlementId())
                .setSettlementTime(optionSettlements.getSettlementTime())
                .setSettlementPrice(DecimalUtil.toBigDecimal(optionSettlements.getSettlementPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setStrikePrice(DecimalUtil.toBigDecimal(optionSettlements.getStrikePrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setQuoteTokenId(symbol.getQuoteTokenId())
                .setQuoteTokenName(symbol.getQuoteTokenName())
                .setBaseTokenId(symbol.getBaseTokenId())
                .setBaseTokenName(symbol.getBaseTokenName())
                .build();
    }

    /**
     * 未实现盈亏 = 平仓价值 - 累计开仓价值open_value
     * <p>
     * 平仓价值（买入平空）= - 平仓价格 * 合约乘数 * 仓位 平仓价值（卖出平多）= + 平仓价格 * 合约乘数 * 仓位
     */
    public static BigDecimal getUnRealisedPnl(BigDecimal openValue, BigDecimal currentPrice, BigDecimal contractMultiplier, BigDecimal total, Boolean isLong) {
        BigDecimal value = currentPrice.multiply(contractMultiplier).multiply(total);

        openValue = isLong ? openValue : openValue.negate();
        value = isLong ? value : value.negate();
        return value.subtract(openValue);

    }

    public static BigDecimal getCoinAvailable(ProfitLossResult longProfitLoss, ProfitLossResult shortProfitLoss) {
        if (longProfitLoss != null) {
            return new BigDecimal(longProfitLoss.getCoinAvailable());
        }
        if (shortProfitLoss != null) {
            return new BigDecimal(shortProfitLoss.getCoinAvailable());
        }
        return BigDecimal.ZERO;
    }

    public static BigDecimal getMargin(ProfitLossResult profitLoss) {
        if (profitLoss == null || StringUtils.isEmpty(profitLoss.getMargin())) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(profitLoss.getMargin());
    }

    public static BigDecimal getOrderMargin(ProfitLossResult profitLoss) {
        if (profitLoss == null || StringUtils.isEmpty(profitLoss.getOrderMargin())) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(profitLoss.getOrderMargin());
    }

    public static BigDecimal getRealisedPnl(ProfitLossResult profitLoss) {
        if (profitLoss == null || StringUtils.isEmpty(profitLoss.getRealisedPnl())) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(profitLoss.getRealisedPnl());
    }

    public static BigDecimal getUnrealisedPnl(ProfitLossResult profitLoss) {
        if (profitLoss == null || StringUtils.isEmpty(profitLoss.getUnrealisedPnl())) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(profitLoss.getUnrealisedPnl());
    }

    public static OrderSideEnum getBHexOrderSide(FuturesOrderSide orderSide) {
        if (orderSide == FuturesOrderSide.BUY_OPEN || orderSide == FuturesOrderSide.BUY_CLOSE) {
            return OrderSideEnum.BUY;
        }
        if (orderSide == FuturesOrderSide.SELL_OPEN || orderSide == FuturesOrderSide.SELL_CLOSE) {
            return OrderSideEnum.SELL;
        }
        return null;
    }

    public static NewPlanOrderRequest createNewPlanOrderRequest(Long accountId, CreateFuturesOrderRequest request,
                                                                SymbolDetail symbolDetail, BigDecimal quotePrice,
                                                                BigDecimal makerFeeRate, BigDecimal takerFeeRate, Decimal makerBonusRate) {
        BigDecimal price = new BigDecimal(request.getPrice());
        BigDecimal triggerPrice = new BigDecimal(request.getTriggerPrice());
        FuturesOrderSide futuresOrderSide = request.getFuturesOrderSide();

        // 目前计划委托只支持限价，因此要校验价格的合法性
        if (triggerPrice.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_ILLEGAL);
        }

        if (request.getPlanOrderType() == PlanOrder.PlanOrderTypeEnum.STOP_COMMON
                && price.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_ILLEGAL);
        }

        BigDecimal originalPrice = price;
        BigDecimal originalTriggerPrice = triggerPrice;
        if (symbolDetail.getIsReverse()) {
            price = getReciprocal(price);
            triggerPrice = getReciprocal(triggerPrice);
            quotePrice = getReciprocal(quotePrice);
            futuresOrderSide = getReverseFuturesSide(futuresOrderSide);
        }

        OrderSideEnum orderSide = FuturesUtil.getBHexOrderSide(futuresOrderSide);
        if (orderSide == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        NewPlanOrderRequest.Builder planOrderRequestBuilder = NewPlanOrderRequest.newBuilder()
                .setAccountId(accountId)
                .setOrderType(PlanOrderTypeEnum.forNumber(request.getPlanOrderTypeValue() - 1))
                .setClientOrderId(request.getClientOrderId())
                .setOrgId(request.getHeader().getOrgId())
                .setExchangeId(request.getExchangeId())
                .setSymbolId(request.getSymbolId())
                .setTriggerPrice(DecimalUtil.fromBigDecimal(triggerPrice))
                .setOriginalTriggerPrice(DecimalUtil.fromBigDecimal(originalTriggerPrice))
                .setPrice(DecimalUtil.fromBigDecimal(price))
                .setQuotePrice(DecimalUtil.fromBigDecimal(quotePrice))
                .setQuotePriceType(PlanOrderQuotePriceType.forNumber(request.getTriggerConditionValue()))
                .setOriginalPrice(DecimalUtil.fromBigDecimal(originalPrice))
                .setQuantity(DecimalUtil.fromBigDecimal(new BigDecimal(request.getQuantity())))
                .setSide(orderSide)
                .setIsClose(request.getIsClose())
                .setCloseType(PlanOrderCloseType.forNumber(request.getCloseTypeValue()))
                .setMakerFeeRate(DecimalUtil.fromBigDecimal(makerFeeRate))
                .setTakerFeeRate(DecimalUtil.fromBigDecimal(takerFeeRate))
                .setMakerBonusRate(makerBonusRate);

        if (StringUtils.isNotEmpty(request.getLeverage())) {
            planOrderRequestBuilder.setLeverage(DecimalUtil.fromBigDecimal(new BigDecimal(request.getLeverage())));
        }
        return planOrderRequestBuilder.build();
    }

    public static PlanOrder toPlanOrder(io.bhex.base.account.PlanOrder order, SymbolDetail symbolDetail) {
        String executedPrice;
        String triggerPrice;
        String price;

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        DecimalStringFormatter priceFormatter = new DecimalStringFormatter(minPricePrecision, BigDecimal.ROUND_DOWN, true);

        if (new BigDecimal(DecimalUtil.toTrimString(order.getExecutedPrice())).compareTo(BigDecimal.ZERO) > 0) {
            executedPrice = symbolDetail.getIsReverse() ? priceFormatter.reciprocalFormat(order.getExecutedPrice()) : priceFormatter.format(order.getExecutedPrice());
        } else {
            executedPrice = priceFormatter.format(order.getExecutedPrice());
        }

        if (order.hasOriginalTriggerPrice()) {
            triggerPrice = priceFormatter.format(order.getOriginalTriggerPrice());
        } else if (new BigDecimal(DecimalUtil.toTrimString(order.getTriggerPrice())).compareTo(BigDecimal.ZERO) > 0) {
            triggerPrice = symbolDetail.getIsReverse() ? priceFormatter.reciprocalFormat(order.getTriggerPrice()) : priceFormatter.format(order.getTriggerPrice());
        } else {
            triggerPrice = priceFormatter.format(order.getTriggerPrice());
        }

        if (order.hasOriginalPrice()) {
            price = priceFormatter.format(order.getOriginalPrice());
        } else if (new BigDecimal(DecimalUtil.toTrimString(order.getTriggerPrice())).compareTo(BigDecimal.ZERO) > 0) {
            price = symbolDetail.getIsReverse() ? priceFormatter.reciprocalFormat(order.getPrice()) : priceFormatter.format(order.getPrice());
        } else {
            price = priceFormatter.format(order.getPrice());
        }

        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        DecimalStringFormatter baseFormatter = new DecimalStringFormatter(basePrecision, BigDecimal.ROUND_DOWN, true);
        DecimalStringFormatter leverageFmt = new DecimalStringFormatter(FUTURES_LEVERAGE_PRECISION);

        PlanOrder.Builder builder = PlanOrder.newBuilder()
                .setOrderId(order.getOrderId())
                .setAccountId(order.getAccountId())
                .setClientOrderId(order.getClientOrderId())
                .setOrgId(order.getOrgId())
                .setExchangeId(order.getExchangeId())
                .setSymbolId(order.getSymbolId())
                .setSymbolName(order.getSymbolId())
                .setBaseTokenId(symbolDetail != null ? symbolDetail.getBaseTokenId() : "")
                .setBaseTokenName(symbolDetail != null ? symbolDetail.getBaseTokenId() : "")
                .setQuoteTokenId(symbolDetail != null ? symbolDetail.getQuoteTokenId() : "")
                .setQuoteTokenName(symbolDetail != null ? symbolDetail.getQuoteTokenId() : "")
                .setLeverage(leverageFmt.format(order.getLeverage()))
                .setPriceType(FuturesPriceType.INPUT)
                .setTriggerPrice(triggerPrice)
                .setPrice(price)
                .setOrigQty(baseFormatter.format(order.getQuantity()))
                .setExecutedPrice(executedPrice)
                .setStatus(toPlanOrderStatus(order))
                .setTime(order.getCreatedTime())
                .setLastUpdated(order.getUpdatedTime())
                .setOrderType(PlanOrder.PlanOrderTypeEnum.forNumber(order.getOrderTypeValue() + 1))
                .setExecutedQuantity(baseFormatter.format(order.getExecutedQuantity()))
                .setExecutedOrderId(order.getExecutedOrderId())
                .setTriggerCondition(PlanOrder.TriggerConditionEnum.forNumber(order.getQuotePriceTypeValue()))
                .setCloseType(PlanOrder.CloseTypeEnum.forNumber(order.getCloseTypeValue()));

        FuturesOrderSide side = toFuturesOrderSide(order.getSide(), order.getIsClose());
        FuturesOrderSide futuresOrderSide = FuturesUtil.toFuturesOrderSide(order.getSide(), order.getIsClose());
        if (symbolDetail.getIsReverse()) {
            futuresOrderSide = getReverseFuturesSide(futuresOrderSide);
        }
        if (side != null) {
            builder.setSide(futuresOrderSide);
        }
        return builder.build();
    }

    private static PlanOrder.PlanOrderStatusEnum toPlanOrderStatus(io.bhex.base.account.PlanOrder order) {
        if (order.getStatus() != io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_FILLED) {
            return PlanOrder.PlanOrderStatusEnum.valueOf(order.getStatus().name());
        } else {
            /*
             * 平台里计划委托单判断委托是否成功是根据status和triggerStatus联合判断
             * 当status=ORDER_FILLED && triggerStatus == SUCCESS 时是委托成功，否则是委托失败
             */
            if (order.getTriggerStatus() == io.bhex.base.account.PlanOrder.PlanOrderTriggerStatusEnum.SUCCESS) {
                return PlanOrder.PlanOrderStatusEnum.ORDER_FILLED;
            } else {
                return PlanOrder.PlanOrderStatusEnum.ORDER_REJECTED;
            }
        }
    }

    public static FuturesOrderSide toFuturesOrderSide(OrderSideEnum side, boolean isClose) {
        if (side == OrderSideEnum.BUY) {
            return isClose ? FuturesOrderSide.BUY_CLOSE : FuturesOrderSide.BUY_OPEN;
        }
        if (side == OrderSideEnum.SELL) {
            return isClose ? FuturesOrderSide.SELL_CLOSE : FuturesOrderSide.SELL_OPEN;
        }
        return null;
    }

    public static InsuranceFund toInsuranceFund(io.bhex.base.account.InsuranceFund fund) {
        return InsuranceFund.newBuilder()
                .setId(fund.getId())
                .setDt(fund.getDt())
                .setTokenId(fund.getTokenId())
                .setAvailable(DecimalUtil.toTrimString(fund.getAvailable()))
                .build();
    }

    public static FuturesOrderSide getReverseFuturesSide(FuturesOrderSide side) {
        if (side == FuturesOrderSide.BUY_OPEN) {
            return FuturesOrderSide.SELL_OPEN;
        }

        if (side == FuturesOrderSide.SELL_OPEN) {
            return FuturesOrderSide.BUY_OPEN;
        }

        if (side == FuturesOrderSide.BUY_CLOSE) {
            return FuturesOrderSide.SELL_CLOSE;
        }

        if (side == FuturesOrderSide.SELL_CLOSE) {
            return FuturesOrderSide.BUY_CLOSE;
        }
        throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
    }

    public static OrderSideEnum getReverseOrderSideEnum(OrderSideEnum side) {
        if (side == OrderSideEnum.BUY) {
            return OrderSideEnum.SELL;
        }

        if (side == OrderSideEnum.SELL) {
            return OrderSideEnum.BUY;
        }

        throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
    }

    public static OrderSide getReverseOrderSide(OrderSideEnum side) {
        if (side == OrderSideEnum.BUY) {
            return OrderSide.SELL;
        }

        if (side == OrderSideEnum.SELL) {
            return OrderSide.BUY;
        }

        throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
    }

    public static OrderSide getOrderSide(OrderSideEnum side) {
        if (side == OrderSideEnum.BUY) {
            return OrderSide.BUY;
        }

        if (side == OrderSideEnum.SELL) {
            return OrderSide.SELL;
        }

        throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
    }

    public static boolean validLeverage(String leverage, TokenFuturesInfo tokenFuturesInfo, BigDecimal takerFeeRate, BigDecimal makerFeeRate) {
        if (StringUtils.isEmpty(leverage)) {
            return true;
        }

        try {
            BigDecimal bLeverage = new BigDecimal(leverage);

            // 配置中取到的最大杠杆
            BigDecimal configMaxLeverage = DecimalUtil.toBigDecimal(tokenFuturesInfo.getMaxLeverage());

            if (bLeverage.compareTo(configMaxLeverage) > 0) {
                return false;
            }

            BigDecimal maxFeeRate = takerFeeRate.abs().max(makerFeeRate.abs());
            if (maxFeeRate.compareTo(BigDecimal.ZERO) == 0) {
                return true;
            }

            // 根据费率推算出来的最大杠杆
            BigDecimal feeRateMaxLeverage = BigDecimal.ONE.divide(maxFeeRate.add(maxFeeRate), 18, RoundingMode.DOWN);
            if (bLeverage.compareTo(feeRateMaxLeverage) < 0) {
                return true;
            } else {
                log.warn("Request leverage {} invalid. feeRateMaxLeverage: {} takerFeeRate: {} makerFeeRate: {}",
                        leverage, feeRateMaxLeverage, takerFeeRate, makerFeeRate);
                return false;
            }
        } catch (Exception e) {
            log.error("validLeverage error", e);
            return true;
        }
    }

    public static BigDecimal getReciprocal(BigDecimal num) {
        if (num.compareTo(BigDecimal.ZERO) == 0) {
            return num;
        } else {
            return BigDecimal.ONE.divide(num, 18, RoundingMode.DOWN);
        }
    }
}




