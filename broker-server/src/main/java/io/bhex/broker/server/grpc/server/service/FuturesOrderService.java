package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.TextFormat;
import io.bhex.base.account.CancelOrderRequest;
import io.bhex.base.account.GetBestOrderRequest;
import io.bhex.base.account.GetBestOrderResponse;
import io.bhex.base.account.GetOrderRequest;
import io.bhex.base.account.*;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.proto.OrderTimeInForceEnum;
import io.bhex.base.proto.*;
import io.bhex.base.rc.OrderSign;
import io.bhex.base.token.MakerBonusConfig;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenFuturesInfo;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.basic.FuturesRiskLimit;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.order.AddMarginRequest;
import io.bhex.broker.grpc.order.AddMarginResponse;
import io.bhex.broker.grpc.order.Fee;
import io.bhex.broker.grpc.order.FundingRate;
import io.bhex.broker.grpc.order.FuturesOrderSetting;
import io.bhex.broker.grpc.order.FuturesPosition;
import io.bhex.broker.grpc.order.FuturesPositionsRequest;
import io.bhex.broker.grpc.order.FuturesPositionsResponse;
import io.bhex.broker.grpc.order.GetFundingRatesRequest;
import io.bhex.broker.grpc.order.GetFundingRatesResponse;
import io.bhex.broker.grpc.order.GetHistoryFundingRatesRequest;
import io.bhex.broker.grpc.order.GetHistoryFundingRatesResponse;
import io.bhex.broker.grpc.order.GetInsuranceFundsRequest;
import io.bhex.broker.grpc.order.GetInsuranceFundsResponse;
import io.bhex.broker.grpc.order.GetOrderSettingRequest;
import io.bhex.broker.grpc.order.GetOrderSettingResponse;
import io.bhex.broker.grpc.order.GetPlanOrderRequest;
import io.bhex.broker.grpc.order.GetRiskLimitRequest;
import io.bhex.broker.grpc.order.GetRiskLimitResponse;
import io.bhex.broker.grpc.order.HistoryFundingRate;
import io.bhex.broker.grpc.order.InsuranceFund;
import io.bhex.broker.grpc.order.Order;
import io.bhex.broker.grpc.order.PlanOrder;
import io.bhex.broker.grpc.order.ReduceMarginRequest;
import io.bhex.broker.grpc.order.ReduceMarginResponse;
import io.bhex.broker.grpc.order.SetOrderSettingRequest;
import io.bhex.broker.grpc.order.SetOrderSettingResponse;
import io.bhex.broker.grpc.order.SetRiskLimitRequest;
import io.bhex.broker.grpc.order.SetRiskLimitResponse;
import io.bhex.broker.grpc.order.*;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import io.bhex.broker.server.elasticsearch.service.ITradeDetailHistoryService;
import io.bhex.broker.server.grpc.client.config.GrpcClientConfig;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcFuturesOrderService;
import io.bhex.broker.server.grpc.client.service.GrpcOrderService;
import io.bhex.broker.server.grpc.server.service.aspect.SiteFunctionLimitSwitchAnnotation;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsBalanceService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.InternationalizationMapper;
import io.bhex.broker.server.util.*;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.bhex.broker.server.domain.BrokerServerConstants.FUTURES_LEVERAGE_PRECISION;
import static io.bhex.broker.server.domain.BrokerServerConstants.FUTURES_MARGIN_RATE_PRECISION;
import static io.bhex.broker.server.grpc.server.service.OrderService.SPECIAL_ORDER_TYPE_PERMISSION;

@Slf4j
@Service
public class FuturesOrderService {

    private static final Histogram FUTURES_ORDER_METRICS = Histogram.build()
            .namespace("broker")
            .subsystem("order")
            .name("futures_order_delay_milliseconds")
            .labelNames("process_name")
            .buckets(BrokerServerConstants.CONTROLLER_TIME_BUCKETS)
            .help("Histogram of stream handle latency in milliseconds")
            .register();

    private static final int MAX_PRICE_PRECISION = 18;

    public static final String ORG_API_CLEAR_CLOSE = "orgApiClearClose";

    @Resource
    GrpcFuturesOrderService grpcFuturesOrderService;

    @Resource
    AccountService accountService;

    @Autowired
    BasicService basicService;

    @Resource
    FuturesQuoteService futuresQuoteService;

    @Resource
    SignUtils signUtils;

    @Resource
    AssetFuturesService assetFuturesService;

    @Resource
    GrpcBalanceService grpcBalanceService;

    @Resource
    GrpcOrderService grpcOrderService;

    @Resource
    private GrpcClientConfig grpcClientConfig;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private InternationalizationMapper internationalizationMapper;
    @Resource
    private StatisticsBalanceService statisticsBalanceService;

    @Resource
    private BrokerFeeService brokerFeeService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private ITradeDetailHistoryService tradeDetailHistoryService;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    static AtomicLong clientIDGen = new AtomicLong(System.currentTimeMillis());

    private static final BigDecimal MAX_PRICE_VALUE = new BigDecimal(9999999);

    private static final BigDecimal MAX_NUMBER = new BigDecimal(9999999999L);

    private static final BigDecimal DEFAULT_MAKER_ORDER_FEE = new BigDecimal("0.0002");
    private static final BigDecimal DEFAULT_TAKER_ORDER_FEE = new BigDecimal("0.00075");
    private static final TradeFeeConfig DEFAULT_TRADE_DEE_CONFIG = TradeFeeConfig.builder()
            .makerFeeRate(DEFAULT_MAKER_ORDER_FEE)
            .takerFeeRate(DEFAULT_TAKER_ORDER_FEE)
            .build();

    private static final BigDecimal DEFAULT_TRANSFER_MAKER_ORDER_FEE = new BigDecimal("0");
    private static final BigDecimal DEFAULT_TRANSFER_TAKER_ORDER_FEE = new BigDecimal("0.0002");
    private static final TradeFeeConfig DEFAULT_TRANSFER_TRADE_DEE_CONFIG = TradeFeeConfig.builder()
            .makerFeeRate(DEFAULT_TRANSFER_MAKER_ORDER_FEE)
            .takerFeeRate(DEFAULT_TRANSFER_TAKER_ORDER_FEE)
            .build();

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_FUTURE_TRADE_KEY, userSwitchGroupKey = BaseConfigConstants.FROZEN_USER_FUTURE_TRADE_GROUP)
    public CreateFuturesOrderResponse newFuturesOrder(CreateFuturesOrderRequest request) {
        Header header = request.getHeader();
        if (header.getUserId() == 842145604923681280L) { //1.禁止这个子账户交易 7070的子账号
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }
        BrokerServerConstants.CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.FUTURES_ORDER_TYPE, header.getPlatform().name()).inc();
        long startTimestamp = System.currentTimeMillis();
        if (header.getPlatform() == Platform.OPENAPI) {
            SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                    BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_API_FUTURE_KEY);
            if (switchStatus.isOpen()) {
                log.info("org:{} ApiNewFutureOrder closed", header.getOrgId());
                throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
            }
        }

        FUTURES_ORDER_METRICS.labels("openapiOrderCheck").observe(System.currentTimeMillis() - startTimestamp);
        long timestamp2 = System.currentTimeMillis();

        BaseBizConfigService.ConfigData allowSymbolsConfig = baseBizConfigService.getBrokerBaseConfig(header.getOrgId(),
                BaseConfigConstants.ALLOW_TRADING_SYMBOLS_GROUP,
                header.getUserId() + "", null);
        if (allowSymbolsConfig != null && StringUtils.isNoneEmpty(allowSymbolsConfig.getValue())) {
            List<String> allowSymbols = Lists.newArrayList(allowSymbolsConfig.getValue().split(","));
            if (!allowSymbols.contains(request.getSymbolId())) {
                log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot trade. not in allowSymbolList", header.getUserId(), request.getSymbolId());
                throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
            }
        }


        Symbol symbol = basicService.getBrokerFuturesSymbol(request.getHeader().getOrgId(), request.getSymbolId());
        if (symbol == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        Long exchangeId = symbol.getExchangeId();
        String symbolId = request.getSymbolId();
        String clientOrderId = request.getClientOrderId();
        FuturesOrderSide futuresOrderSide = request.getFuturesOrderSide();
        PlanOrder.FuturesOrderType futuresOrderType = request.getFuturesOrderType();
        String qty = request.getQuantity();
        String leverage = request.getLeverage();
        boolean isClose = request.getIsClose();

        Long accountId = accountService.getFuturesIndexAccountId(header, request.getAccountIndex());
        if (futuresOrderSide == null || futuresOrderSide == FuturesOrderSide.UNKNOWN_FUTURES_ORDER_SIDE) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        if (header.getPlatform() == Platform.OPENAPI && symbol.getForbidOpenapiTrade() == 1
                && brokerFeeService.getCachedTradeFeeConfig(header.getOrgId(), symbol.getExchangeId(), request.getSymbolId(), accountId) == null) {
            throw new BrokerException(BrokerErrorCode.SYMBOL_OPENAPI_TRADE_FORBIDDEN);
        }

        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(header.getOrgId(), symbolId);
        if (symbolDetail == null) {
            log.warn("getSymbolDetailFutures null. exchangeId:{}, symbolId:{}", exchangeId, symbolId);
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        //重写exchangeId
        exchangeId = symbolDetail.getExchangeId();

        // 校验币对是否允许交易
        if (!checkAllowTrade(header.getOrgId(), symbolId)) {
            log.warn("symbol: {} not allow trade.", symbolId);
            throw new BrokerException(BrokerErrorCode.ORDER_FUTURES_TRADE_CLOSED);
        }

        FUTURES_ORDER_METRICS.labels("switchCheck").observe(System.currentTimeMillis() - timestamp2);
        long timestamp3 = System.currentTimeMillis();

        TokenFuturesInfo tokenFuturesInfo = basicService.getTokenFuturesInfoMap().get(symbolId);
        if (tokenFuturesInfo == null) {
            log.warn("get TokenFuturesInfo null. exchangeId:{}, symbolId:{}", exchangeId, symbolId);
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        if (futuresOrderType == null || futuresOrderType == PlanOrder.FuturesOrderType.UNKNOWN_ORDER_TYPE) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        //下单价格
        BigDecimal price = futuresQuoteService.getFutureOrderPrice(futuresOrderSide, exchangeId, symbolDetail,
                request.getFuturesPriceType(),
                CommonUtil.toBigDecimal(request.getPrice()),
                CommonUtil.toBigDecimal(request.getTriggerPrice()),
                isClose, header.getOrgId());

        FUTURES_ORDER_METRICS.labels("getOrderPrice").observe(System.currentTimeMillis() - timestamp3);
        long timestamp4 = System.currentTimeMillis();

        //如果是反向期货
        if (symbolDetail.getIsReverse()) {
            futuresOrderSide = FuturesUtil.getReverseFuturesSide(futuresOrderSide);
        }

        if (futuresOrderSide == FuturesOrderSide.BUY_OPEN || futuresOrderSide == FuturesOrderSide.SELL_OPEN) {
            BaseBizConfigService.ConfigData disableBuySymbolsConfig = baseBizConfigService.getBrokerBaseConfig(header.getOrgId(),
                    BaseConfigConstants.DISABLE_USER_TRADING_SYMBOLS_GROUP, header.getUserId() + "-OPEN", null);
            if (disableBuySymbolsConfig != null && StringUtils.isNotEmpty(disableBuySymbolsConfig.getValue())) {
                List<String> disableSymbols = Lists.newArrayList(disableBuySymbolsConfig.getValue().split(","));
                if (disableSymbols.contains(symbolId)) {
                    log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot buy. in disableBuySymbolList", header.getUserId(), symbolId);
                    throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
                }
            }
        }

        if (futuresOrderSide == FuturesOrderSide.BUY_CLOSE || futuresOrderSide == FuturesOrderSide.SELL_CLOSE) {
            BaseBizConfigService.ConfigData disableSellSymbolsConfig = baseBizConfigService.getBrokerBaseConfig(header.getOrgId(),
                    BaseConfigConstants.DISABLE_USER_TRADING_SYMBOLS_GROUP, header.getUserId() + "-CLOSE", null);
            if (disableSellSymbolsConfig != null && StringUtils.isNotEmpty(disableSellSymbolsConfig.getValue())) {
                List<String> disableSymbols = Lists.newArrayList(disableSellSymbolsConfig.getValue().split(","));
                if (disableSymbols.contains(symbolId)) {
                    log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot sell. in disableSellSymbolList", header.getUserId(), symbolId);
                    throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
                }
            }
        }


        //下单方向
        OrderSideEnum orderSide = FuturesUtil.getBHexOrderSide(futuresOrderSide);
        if (orderSide == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        //判断期权是否交割
        preCheckSettlement(symbolId);

        //校验是否禁售,该用户是否在白名单内
        preCheckAccount(header, exchangeId, symbolId, futuresOrderSide);

        //校验限价单的下单价格: 价格校验   最大值、最小值、小数位数
        validPrice(request, price, symbolDetail);

        //校验限价单的下单数量或者市价卖单的下单数量: 数量校验   最大值、最小值
        BigDecimal quantity = new BigDecimal(qty).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        validQuantity(quantity, symbolDetail);

        NewOrderRequest.Builder builder = NewOrderRequest.newBuilder();

        //取倒数 精度 = basePrecision + quotePrecision + 1
        BigDecimal originPrice = price;
        if (symbolDetail.getIsReverse() && price != null && price.compareTo(BigDecimal.ZERO) > 0) {
            price = BigDecimal.ONE.divide(price, MAX_PRICE_PRECISION, RoundingMode.DOWN);
        }

        builder.setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setExchangeId(exchangeId)
                .setSymbolId(symbolId)
                .setClientOrderId(clientOrderId)
                .setSide(orderSide)
                .setPrice(DecimalUtil.fromBigDecimal(price))
                .setIsClose(isClose)
                .setTimeInForce(OrderTimeInForceEnum.valueOf(request.getTimeInForce().name()));

        builder.setOrderType(futuresOrderType == PlanOrder.FuturesOrderType.STOP ? OrderTypeEnum.STOP_LIMIT : OrderTypeEnum.LIMIT);
        builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
        builder.setLeverage(DecimalUtil.fromBigDecimal(CommonUtil.toBigDecimal(leverage, BigDecimal.ZERO)));
        builder.setOriginalPrice(DecimalUtil.fromBigDecimal(originPrice));

        // 校验最小交易额
        validTradeAmount(request, price, builder, quantity, symbolDetail);

        FUTURES_ORDER_METRICS.labels("paramCheck").observe(System.currentTimeMillis() - timestamp4);
        long timestamp5 = System.currentTimeMillis();

        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        builder.setSignTime(signTime).setSignNonce(signNonce);


        // 如果是openapi的请求，忽略用户的订单设置
        if (header.getPlatform() == Platform.OPENAPI || !request.getExtraFlag().equals(FuturesOrderExtraFlag.USE_SETTINGS)) {
            if (request.getExtraFlag() == FuturesOrderExtraFlag.MAKER_ONLY) {
                // 如果openapi设置MAKER_ONLY选项，表示是只做maker单
                builder.setOrderType(OrderTypeEnum.LIMIT_MAKER);
                builder.setTimeInForce(OrderTimeInForceEnum.GTC);
            } else if (request.getExtraFlag() == FuturesOrderExtraFlag.LIMIT_FREE_FLAG) {
                // 如果openapi设置LIMIT_FREE_FLAG，对应平台的订单类型为LIMIT_FREE
                if (!commonIniService.getStringValueOrDefault(header.getOrgId(), SPECIAL_ORDER_TYPE_PERMISSION, "").contains(String.valueOf(header.getUserId()))) {
                    log.warn("{}-{} order with {}, but not in white list", header.getOrgId(), header.getUserId(), OrderTypeEnum.LIMIT_FREE);
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                }
                builder.setOrderType(OrderTypeEnum.LIMIT_FREE);
            } else if (request.getExtraFlag() == FuturesOrderExtraFlag.LIMIT_MAKER_FREE_FLAG) {
                // LIMIT_MAKER_FREE_FLAG，对应平台的订单类型为LIMIT_MAKER_FREE
                if (!commonIniService.getStringValueOrDefault(header.getOrgId(), SPECIAL_ORDER_TYPE_PERMISSION, "").contains(String.valueOf(header.getUserId()))) {
                    log.warn("{}-{} order with {}, but not in white list", header.getOrgId(), header.getUserId(), OrderTypeEnum.LIMIT_MAKER_FREE);
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                }
                builder.setOrderType(OrderTypeEnum.LIMIT_MAKER_FREE);
            }
        } else {
            // 按用户设置配置订单类型：只做maker，FOK，IOC。默认为GTC
            processOrderType(header, builder);
        }
        FUTURES_ORDER_METRICS.labels("handleOrderType").observe(System.currentTimeMillis() - timestamp5);
        long timestamp6 = System.currentTimeMillis();
        // 设置用户下单价格类型：用户输入价, 对手价, 排队价, 超价, 市价
        processPriceType(builder, request.getFuturesPriceType());

        OrderSign orderSign = OrderSign.newBuilder()
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setExchangeId(exchangeId)
                .setSymbolId(symbolId)
                .setClientOrderId(clientOrderId)
                .setOrderType(builder.getOrderTypeValue())
                .setSide(builder.getSideValue())
                .setPrice(DecimalUtil.toTrimString(builder.getPrice()))
                .setAmount(DecimalUtil.toTrimString(builder.getAmount()))
                .setQuantity(DecimalUtil.toTrimString(builder.getQuantity()))
                .setTimeInForce(builder.getTimeInForce().getNumber())
                .setSignTime(signTime)
                .setSignNonce(signNonce)
                .build();

        String sign;
        try {
            sign = signUtils.sign(orderSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            log.error("create order add sign occurred a error orgId:{}  param:{}",
                    header.getOrgId(), TextFormat.shortDebugString(orderSign), e);
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
        builder.setSignTime(signTime).setSignNonce(signNonce).setSignBroker(sign);

        FUTURES_ORDER_METRICS.labels("orderSign").observe(System.currentTimeMillis() - timestamp6);
        long timestamp7 = System.currentTimeMillis();

        // TODO:
        MakerBonusConfig config = basicService.getMakerBonusConfig(header.getOrgId(), symbolId);
        BigDecimal makerBonusRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMakerBonusRate());
        BigDecimal minInterestFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinInterestFeeRate());
        BigDecimal minTakerFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinTakerFeeRate());

        OrderSide side = symbolDetail.getIsReverse() ? orderSide == OrderSideEnum.BUY ? OrderSide.SELL : OrderSide.BUY : orderSide == OrderSideEnum.BUY ? OrderSide.BUY : OrderSide.SELL;
        boolean isTransferOrder = false;
        if (symbolDetail.getExchangeId() != symbolDetail.getMatchExchangeId()) {
            isTransferOrder = Boolean.TRUE;
        }
        TradeFeeConfig tradeFeeConfig = brokerFeeService.getOrderTradeFeeConfig(header.getOrgId(), symbol.getExchangeId(), symbolId, isTransferOrder, header.getUserId(), accountId, AccountType.FUTURES,
                side, DEFAULT_TRADE_DEE_CONFIG, DEFAULT_TRANSFER_TRADE_DEE_CONFIG, makerBonusRate, minInterestFeeRate, minTakerFeeRate);

        if (futuresOrderType == PlanOrder.FuturesOrderType.STOP) {
            BigDecimal quotePrice = getPlanOrderQuotePrice(symbolDetail, request);
            // 校验止盈止损单
            PlanOrder.PlanOrderTypeEnum planOrderType = request.getPlanOrderType();
            if (planOrderType == PlanOrder.PlanOrderTypeEnum.UNKNOWN_PLAN_ORDER_TYPE) {
                throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_REJECTED);
            }

            if (planOrderType != PlanOrder.PlanOrderTypeEnum.STOP_COMMON) {
                if (!isClose) {
                    // 只有平仓单才能下止盈止损
                    throw new BrokerException(BrokerErrorCode.ORDER_SPL_REJECT);
                }

                BigDecimal positionAvailable = getBhFuturesPositonAvailable(header.getOrgId(), accountId, symbolDetail, planOrderType);
                if (positionAvailable.subtract(quantity).compareTo(BigDecimal.ZERO) < 0) {
                    throw new BrokerException(BrokerErrorCode.ORDER_SPL_POSITION_NOT_ENOUGH);
                }

                // 校验止盈止损的触发价格
                checkStopProfitLossTriggerPrice(request, quotePrice);
            }

            NewPlanOrderRequest planOrderRequest = FuturesUtil.createNewPlanOrderRequest(accountId, request, symbolDetail, quotePrice,
                    tradeFeeConfig.getMakerFeeRate(), tradeFeeConfig.getTakerFeeRate(),
                    tradeFeeConfig.getMakerFeeRate().compareTo(BigDecimal.ZERO) < 0 ? DecimalUtil.fromBigDecimal(makerBonusRate) : DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
            FUTURES_ORDER_METRICS.labels("feeRateSetting").observe(System.currentTimeMillis() - timestamp7);
            long timestamp8 = System.currentTimeMillis();
            NewPlanOrderReply planOrderReply = grpcFuturesOrderService.createFuturesPlanOrder(planOrderRequest);
            BrokerServerConstants.SUCCESS_CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.FUTURES_ORDER_TYPE, header.getPlatform().name()).inc();
            FUTURES_ORDER_METRICS.labels("planOrderRequest").observe(System.currentTimeMillis() - timestamp8);
            FUTURES_ORDER_METRICS.labels("allProcess").observe(System.currentTimeMillis() - startTimestamp);
            return CreateFuturesOrderResponse.newBuilder()
                    .setPlanOrder(FuturesUtil.toPlanOrder(planOrderReply.getOrder(), symbolDetail))
                    .build();
        }
        if (request.getFuturesPriceType() == FuturesPriceType.MARKET_PRICE) {//市价
            builder.setTimeInForce(OrderTimeInForceEnum.IOC);
            // 如果是市价平仓单，发送给平台的订单类型为MARKET_OF_BASE，并且不设置价格
            if (isClose) {
                builder.setOrderType(OrderTypeEnum.MARKET_OF_BASE);
                builder.setPrice(DecimalUtil.fromLong(0L));
            }
        }

        builder.setOrderSource(basicService.getOrderSource(header.getOrgId(), header.getPlatform(), request.getOrderSource()));
        builder.setMakerFeeRate(DecimalUtil.fromBigDecimal(tradeFeeConfig.getMakerFeeRate()));//默认费率
        builder.setTakerFeeRate(DecimalUtil.fromBigDecimal(tradeFeeConfig.getTakerFeeRate()));//默认费率
        builder.setMakerBonusRate(tradeFeeConfig.getMakerFeeRate().compareTo(BigDecimal.ZERO) < 0 ?
                DecimalUtil.fromBigDecimal(makerBonusRate) : DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
        log.info("[Futures] Handel broker fee, orgId:{} userId:{} accountId:{} clientOrderId:{} symbolId:{} makerFeeRate:{} takerFeeRate:{} makerBonusRate:{} minTakerFeeRate:{}",
                header.getOrgId(), header.getUserId(), accountId, clientOrderId, symbolId, builder.getMakerFeeRate().getStr(), builder.getTakerFeeRate().getStr(),
                makerBonusRate.toPlainString(), minTakerFeeRate.toPlainString());
        FUTURES_ORDER_METRICS.labels("feeRateSetting").observe(System.currentTimeMillis() - timestamp7);
        long timestamp8 = System.currentTimeMillis();

        //校验数量价格精度
        BigDecimalUtil.checkParamScale(price != null ? price.toPlainString() : "", quantity != null ? quantity.toPlainString() : "");

        // 杠杆合法性校验
        if (!FuturesUtil.validLeverage(leverage, tokenFuturesInfo, DecimalUtil.toBigDecimal(builder.getTakerFeeRate()),
                DecimalUtil.toBigDecimal(builder.getMakerFeeRate()))) {
            log.warn("clientOrderId: {} leverage: {} not valid. leverage range:{} ",
                    clientOrderId, leverage, tokenFuturesInfo.getLeverageRange());
            throw new BrokerException(BrokerErrorCode.ORDER_FUTURES_LEVERAGE_INVALID);
        }

        NewOrderRequest newOrderRequest = builder.build();
        NewOrderReply reply = grpcFuturesOrderService.createFuturesOrder(newOrderRequest, symbolDetail);
        FUTURES_ORDER_METRICS.labels("orderRequest").observe(System.currentTimeMillis() - timestamp8);
        if (reply.getStatus().equals(OrderStatusEnum.REJECTED) && builder.getOrderType().equals(OrderTypeEnum.LIMIT_MAKER)) {
            // 当为Limit Maker单时，撮合拒单，一定是下MAKER单失败。
            throw new BrokerException(BrokerErrorCode.ORDER_LIMIT_MAKER_FAILED);
        }
        FUTURES_ORDER_METRICS.labels("allProcess").observe(System.currentTimeMillis() - startTimestamp);
        BrokerServerConstants.SUCCESS_CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.FUTURES_ORDER_TYPE, header.getPlatform().name()).inc();
        return CreateFuturesOrderResponse.newBuilder().setOrder(getFuturesOrder(reply.getOrder(), symbolDetail, exchangeId)).build();
    }

    /**
     * 获取计划委托下单时的行情价格（最新价或者是指数价）
     */
    private BigDecimal getPlanOrderQuotePrice(SymbolDetail symbol, CreateFuturesOrderRequest request) {
        return getPlanOrderQuotePrice(symbol, request.getTriggerCondition(), request.getHeader().getOrgId());
    }

    private BigDecimal getPlanOrderQuotePrice(SymbolDetail symbol, PlanOrder.TriggerConditionEnum triggerCondition, Long orgId) {
        BigDecimal quotePrice;
        switch (triggerCondition) {
            case CONDITION_INDICES:
                quotePrice = futuresQuoteService.getIndices(symbol.getDisplayIndexToken(), orgId);
                break;
            case CONDITION_REALTIME:
            default:
                quotePrice = futuresQuoteService.getCurrentPrice(symbol.getSymbolId(), symbol.getExchangeId(), orgId);
        }

        if (log.isDebugEnabled()) {
            log.debug("getPlanOrderQuotePrice - {} condition: {}", quotePrice, triggerCondition.name());
        }
        return quotePrice;
    }

    /**
     * 检查止盈止损的价格是否合理
     */
    private void checkStopProfitLossTriggerPrice(CreateFuturesOrderRequest request, BigDecimal quotePrice) {
        if (request.getPlanOrderType() == PlanOrder.PlanOrderTypeEnum.UNKNOWN_PLAN_ORDER_TYPE ||
                request.getPlanOrderType() == PlanOrder.PlanOrderTypeEnum.STOP_COMMON) {
            return;
        }

        BigDecimal triggerPrice = new BigDecimal(request.getTriggerPrice());
        checkStopProfitLossTriggerPrice(triggerPrice, request.getPlanOrderType(), quotePrice);
    }

    private void checkStopProfitLossTriggerPrice(BigDecimal triggerPrice, PlanOrder.PlanOrderTypeEnum planOrderType,
                                                 BigDecimal quotePrice) {
        if (quotePrice == null || quotePrice.compareTo(BigDecimal.ZERO) == 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_ILLEGAL);
        }

        switch (planOrderType) {
            case STOP_LONG_PROFIT:
                if (triggerPrice.compareTo(quotePrice) <= 0) {
                    throw new BrokerException(BrokerErrorCode.ORDER_SPL_ERR_LONG_STOP_PROFIT_PRICE);
                }
                break;
            case STOP_SHORT_LOSS:
                if (triggerPrice.compareTo(quotePrice) <= 0) {
                    throw new BrokerException(BrokerErrorCode.ORDER_SPL_ERR_SHORT_STOP_LOSS_PRICE);
                }
                break;
            case STOP_LONG_LOSS:
                if (triggerPrice.compareTo(quotePrice) >= 0) {
                    throw new BrokerException(BrokerErrorCode.ORDER_SPL_ERR_LONG_STOP_LOSS_PRICE);
                }
                break;
            case STOP_SHORT_PROFIT:
                if (triggerPrice.compareTo(quotePrice) >= 0) {
                    throw new BrokerException(BrokerErrorCode.ORDER_SPL_ERR_SHORT_STOP_PROFIT_PRICE);
                }
                break;
        }
    }

    private BigDecimal getBhFuturesPositonAvailable(Long orgId, Long accountId, SymbolDetail symbolDetail, PlanOrder.PlanOrderTypeEnum planOrderType) {
        // 检查当前账户的仓位
        int isLong;
        if (symbolDetail.getIsReverse()) {
            isLong = (planOrderType == PlanOrder.PlanOrderTypeEnum.STOP_SHORT_PROFIT
                    || planOrderType == PlanOrder.PlanOrderTypeEnum.STOP_SHORT_LOSS) ? 1 : 0;
        } else {
            isLong = (planOrderType == PlanOrder.PlanOrderTypeEnum.STOP_LONG_PROFIT
                    || planOrderType == PlanOrder.PlanOrderTypeEnum.STOP_LONG_LOSS) ? 1 : 0;
        }
        io.bhex.base.account.FuturesPositionsRequest request =
                io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                        .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                        .addTokenIds(symbolDetail.getSymbolId())
                        .setAccountId(accountId)
                        .setIsLong(isLong)
                        .build();
        io.bhex.base.account.FuturesPositionsResponse response = grpcFuturesOrderService.getFuturesPositions(request);
        if (response.getPositionsCount() <= 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_SPL_NO_POSITION);
        }

        return DecimalUtil.toBigDecimal(response.getPositions(0).getAvailable());
    }

    /**
     * 根据broker的symbol配置来决定是否允许交易
     */
    private boolean checkAllowTrade(Long orgId, String symbolId) {
        Symbol brokerSymbol = basicService.getBrokerFuturesSymbol(orgId, symbolId);
        if (brokerSymbol == null) {
            log.error("checkAllowTrade error. get broker symbol null. orgId: {} symbolId: {}", orgId, symbolId);
            return false;
        }

        return brokerSymbol.getAllowTrade().equals(BrokerServerConstants.ALLOW_STATUS);
    }


    private void processOrderType(Header header, NewOrderRequest.Builder builder) {
        GetOrderSettingResponse orderSettingResponse = getOrderSetting(header);
        if (Objects.nonNull(orderSettingResponse)) {
            FuturesOrderSetting orderSetting = orderSettingResponse.getOrderSetting();
            String timeInForce = orderSetting.getTimeInForce();
            if (StringUtils.isEmpty(timeInForce)) {
                return;
            } else {
                switch (timeInForce) {
                    case "IOC":
                        builder.setOrderType(OrderTypeEnum.LIMIT);
                        builder.setTimeInForce(OrderTimeInForceEnum.IOC);
                        break;
                    case "FOK":
                        builder.setOrderType(OrderTypeEnum.LIMIT);
                        builder.setTimeInForce(OrderTimeInForceEnum.FOK);
                        break;
                    case "MAKER":
                        builder.setOrderType(OrderTypeEnum.LIMIT_MAKER);
                        builder.setTimeInForce(OrderTimeInForceEnum.GTC);
                        break;
                    default:
                        builder.setOrderType(OrderTypeEnum.LIMIT);
                        builder.setTimeInForce(OrderTimeInForceEnum.GTC);

                }
            }

        }
    }

    private void processPriceType(NewOrderRequest.Builder builder, FuturesPriceType priceType) {
        if (Objects.isNull(priceType)) {
            builder.setExtraFlag(NewOrderRequest.ExtraFlagEnum.DEFAULT);
            return;
        }
        switch (priceType) {
            case INPUT:
                builder.setExtraFlag(NewOrderRequest.ExtraFlagEnum.INPUT);
                break;
            case OPPONENT:
                builder.setExtraFlag(NewOrderRequest.ExtraFlagEnum.OPPONENT);
                break;
            case QUEUE:
                builder.setExtraFlag(NewOrderRequest.ExtraFlagEnum.QUEUE);
                break;
            case OVER:
                builder.setExtraFlag(NewOrderRequest.ExtraFlagEnum.OVER);
                break;
            case MARKET_PRICE:
                builder.setExtraFlag(NewOrderRequest.ExtraFlagEnum.MARKET_PRICE);
                break;
            default:
                builder.setExtraFlag(NewOrderRequest.ExtraFlagEnum.DEFAULT);
                break;
        }
    }

    public FuturesPriceType getPriceType(NewOrderRequest.ExtraFlagEnum extraFlagEnum) {
        if (Objects.isNull(extraFlagEnum)) {
            return FuturesPriceType.INPUT;
        }
        switch (extraFlagEnum) {
            case INPUT:
                return FuturesPriceType.INPUT;
            case OPPONENT:
                return FuturesPriceType.OPPONENT;
            case QUEUE:
                return FuturesPriceType.QUEUE;
            case OVER:
                return FuturesPriceType.OVER;
            case MARKET_PRICE:
                return FuturesPriceType.MARKET_PRICE;
            default:
                return FuturesPriceType.INPUT;
        }
    }

    private void validPrice(CreateFuturesOrderRequest request, BigDecimal price, SymbolDetail symbol) {
        if (request.getFuturesPriceType().equals(FuturesPriceType.MARKET_PRICE) && request.getIsClose()) {
            // 如果是市价平仓单，不校验价格
            return;
        }

        if (price == null) {
            log.info("validPrice price is null. symbolId:{}", symbol.getSymbolId());
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_TOO_SMALL);
        }
        if (price.compareTo(MAX_PRICE_VALUE) > 0) {
            log.info("validPrice price is bigger than MAX_NUMBER. price:{}", price.stripTrailingZeros().toPlainString());
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_TOO_HIGH);
        }
        if (price.compareTo(DecimalUtil.toBigDecimal(symbol.getMinPricePrecision())) < 0) {
            log.info("validPrice price is small than minPricePrecision. price:{}, minPricePrecision:{}",
                    price.stripTrailingZeros().toPlainString(), DecimalUtil.toBigDecimal(symbol.getMinPricePrecision()).stripTrailingZeros().toPlainString());
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_TOO_SMALL);
        }

        int symbolPrecision = DecimalUtil.toBigDecimal(symbol.getMinPricePrecision()).stripTrailingZeros().scale();
        int orderPrecision = price.stripTrailingZeros().scale();
        if (orderPrecision > symbolPrecision) {
            log.info("validPrice precision is larger than config. price:{}, minPricePrecision:{}",
                    price.stripTrailingZeros().toPlainString(), DecimalUtil.toBigDecimal(symbol.getMinPricePrecision()).stripTrailingZeros().toPlainString());
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_PRECISION_TOO_LONG);
        }
    }

    private void validQuantity(BigDecimal quantity, SymbolDetail symbol) {
        if (quantity.compareTo(MAX_NUMBER) > 0) {
            log.info("validQuantity quantity is bigger than MAX_NUMBER. quantity:{}", quantity.stripTrailingZeros().toPlainString());
            throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_TOO_BIG);
        }
        if (quantity.compareTo(DecimalUtil.toBigDecimal(symbol.getMinTradeQuantity())) < 0) {
            log.info("validQuantity price is small than minTradeQuantity. quantity:{}, minTradeQuantity:{}",
                    quantity.stripTrailingZeros().toPlainString(), DecimalUtil.toBigDecimal(symbol.getMinTradeQuantity()).stripTrailingZeros().toPlainString());

            String message = DecimalUtil.toBigDecimal(symbol.getMinTradeQuantity()).stripTrailingZeros().toPlainString();
            throw new BrokerException(BrokerErrorCode.OPTION_ORDER_QUANTITY_TOO_SMALL, new Object[]{message});
        }
        int symbolPrecision = DecimalUtil.toBigDecimal(symbol.getBasePrecision()).stripTrailingZeros().scale();
        int orderPrecision = quantity.stripTrailingZeros().scale();
        if (orderPrecision > symbolPrecision) {
            log.info("[order quantity precision config alert] validQuantity precision is larger than config. quantity:{}, minTradeQuantity:{}",
                    quantity.stripTrailingZeros().toPlainString(), DecimalUtil.toBigDecimal(symbol.getMinTradeQuantity()).stripTrailingZeros().toPlainString());
            throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_PRECISION_TOO_LONG);
        }
    }

    private void validTradeAmount(CreateFuturesOrderRequest request, BigDecimal price,
                                  NewOrderRequest.Builder builder, BigDecimal quantity, SymbolDetail symbolDetail) {
        if (request.getFuturesPriceType().equals(FuturesPriceType.MARKET_PRICE) && request.getIsClose()) {
            // 如果是市价平仓单，不校验最小交易金额
            return;
        }

        BigDecimal minTradeAmount = DecimalUtil.toBigDecimal(symbolDetail.getMinTradeAmount());
        BigDecimal multiplier = DecimalUtil.toBigDecimal(symbolDetail.getFuturesMultiplier());

        // 价格 * 数量 * 合约乘数 * 1.1
        // 注：如果是反向合约，这里的价格需要取倒数
        BigDecimal amount = price.multiply(quantity).multiply(multiplier).multiply(BigDecimal.valueOf(1.1D));
        if (amount.compareTo(minTradeAmount) < 0) {
            log.warn("create order transaction amount:{} less than symbol required:{}. order:{}",
                    amount.toPlainString(),
                    minTradeAmount.toPlainString(),
                    TextFormat.shortDebugString(builder.build())
            );
            throw new BrokerException(BrokerErrorCode.OPTION_ORDER_AMOUNT_TOO_SMALL, new Object[]{
                    minTradeAmount.stripTrailingZeros().toPlainString()
            });
        }
    }

    private void preCheckAccount(Header header, Long exchangeId, String symbolId, FuturesOrderSide orderSide) {
//        if (orderSide.getNumber() == OrderSideEnum.SELL.getNumber()) {
//            GetBanSellConfigResponse isOk
//                    = getBanSellConfig(header.getUserId(), header.getOrgId(), exchangeId, symbolId);
//            if (isOk == null || !isOk.getCanSell()) {
//                throw new BrokerException(BrokerErrorCode.ORDER_HAS_BEEN_FILLED);
//            }
//        }
    }

    private void preCheckSettlement(String symbolId) {
//        TokenOptionInfo tokenOptionInfo = basicService.getTokenOptionInfo(symbolId);
//        if (tokenOptionInfo == null) {
//            log.warn("create order cannot find option token.");
//            throw new BrokerException(BrokerErrorCode.OPTION_NOT_EXIST);
//        }
//
//        if (tokenOptionInfo.getSettlementDate() < new Date().getTime()) {
//            log.warn("create order filed The option has expired.");
//            throw new BrokerException(BrokerErrorCode.OPTION_HAS_EXPIRED);
//        }
    }

    public CancelFuturesOrderResponse cancelFuturesOrder(Header header, int accountIndex,
                                                         Long orderId, String clientOrderId, PlanOrder.FuturesOrderType futuresOrderType) {
        Long accountId = accountService.getFuturesIndexAccountId(header, accountIndex);
        if (futuresOrderType == PlanOrder.FuturesOrderType.STOP) {
            CancelPlanOrderRequest planOrderRequest = CancelPlanOrderRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                    .setAccountId(accountId)
                    .setClientOrderId(clientOrderId)
                    .setOrderId(orderId)
                    .build();
            CancelPlanOrderReply reply = grpcFuturesOrderService.cancelFuturesPlanOrder(planOrderRequest);

            io.bhex.base.account.PlanOrder order = reply.getOrder();
            Long exchangeId = basicService.getOrgExchangeFuturesMap().get(String.format("%s_%s", order.getOrgId(), order.getSymbolId()));
            SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, order.getSymbolId());

            return CancelFuturesOrderResponse.newBuilder()
                    .setPlanOrder(FuturesUtil.toPlanOrder(reply.getOrder(), symbolDetail))
                    .build();
        }

        if (futuresOrderType == PlanOrder.FuturesOrderType.LIMIT) {
            long signTime = System.currentTimeMillis() / 1000;
            String signNonce = UUID.randomUUID().toString();

            io.bhex.base.account.CancelOrderRequest.Builder builder = io.bhex.base.account.CancelOrderRequest.newBuilder()
                    .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                    .setAccountId(accountId)
                    .setOrderId(orderId)
                    .setClientOrderId(clientOrderId)
                    .setReqStartTime(header.getRequestTime());

            io.bhex.base.rc.CancelOrderRequest orderSign = io.bhex.base.rc.CancelOrderRequest.newBuilder()
                    .setAccountId(accountId)
                    .setClientOrderId(Strings.nullToEmpty(clientOrderId))
                    .setOrderId(orderId)
                    .setSignTime(signTime)
                    .setSignNonce(signNonce)
                    .build();

            String sign;
            try {
                sign = signUtils.sign(orderSign.toByteArray(), header.getOrgId());
            } catch (Exception e) {
                log.error("cancel order add sign occurred a error", e);
                throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED);
            }

            builder.setOrderType(OrderTypeEnum.LIMIT);
            builder.setSignTime(signTime)
                    .setSignNonce(signNonce)
                    .setSignBorker(sign);
            CancelOrderRequest request = builder.build();
            CancelOrderReply reply = grpcFuturesOrderService.cancelFuturesOrder(builder.build());
            io.bhex.base.account.Order order = reply.getOrder();
            Long exchangeId = basicService.getOrgExchangeFuturesMap().get(String.format("%s_%s", order.getOrgId(), order.getSymbolId()));
            SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, order.getSymbolId());
            if (symbolDetail == null) {
                throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
            }
            return CancelFuturesOrderResponse.newBuilder().setOrder(getFuturesOrder(reply.getOrder(), symbolDetail, exchangeId)).build();
        }
        return CancelFuturesOrderResponse.newBuilder().build();
    }

    public BatchCancelOrderResponse batchCancelFuturesOrder(Header header, AccountTypeEnum accountTypeEnum, int accountIndex,
                                                            List<String> symbolIds, OrderSide orderSide) {
        io.bhex.base.account.BatchCancelOrderRequest.Builder builder = io.bhex.base.account.BatchCancelOrderRequest.newBuilder();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getFuturesAccountId(header);
        Long accountId = accountService.getFuturesIndexAccountId(header, accountIndex);
        builder.setAccountId(accountId);
        builder.setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header));
        if (symbolIds != null && symbolIds.size() > 0) {
            builder.addAllSymbolIds(symbolIds);
        }
        if (orderSide != null && orderSide != OrderSide.UNKNOWN_ORDER_SIDE) {
            builder.addSides(OrderSideEnum.valueOf(orderSide.name()));
        }
        grpcFuturesOrderService.batchCancelFuturesOrder(builder.build());
        return BatchCancelOrderResponse.getDefaultInstance();
    }

    public SetRiskLimitResponse setRiskLimit(SetRiskLimitRequest request) {
        if (!basicService.filterRiskLimit(request.getHeader(), request.getRiskLimitId(), false)) {
            log.warn("user {} riskLimitId: {} not in whiteList", request.getHeader().getUserId(), request.getRiskLimitId());
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        // 如果是反向合约，需要把请求的is_long取反，获取真正的值
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                request.getHeader().getOrgId(), request.getSymbolId());
        boolean realIsLong = symbolDetail.getIsReverse() != request.getIsLong();
        Long futuresAccountId = accountService.getFuturesIndexAccountId(request.getHeader(), request.getAccountIndex());
        io.bhex.base.account.SetRiskLimitRequest req = io.bhex.base.account.SetRiskLimitRequest.newBuilder()
                .setAccountId(futuresAccountId)
                .setSymbolId(request.getSymbolId())
                .setRiskLimitId(request.getRiskLimitId())
                .setIsLong(realIsLong)
                .setBrokerId(request.getHeader().getOrgId())
                .setExchangeId(symbolDetail.getExchangeId())
                .build();
        io.bhex.base.account.SetRiskLimitResponse response = grpcFuturesOrderService.setRiskLimit(req);
        return SetRiskLimitResponse.newBuilder()
                .setSuccess(response.getSuccess())
                .build();
    }

    public SetOrderSettingResponse setOrderSetting(SetOrderSettingRequest request) {
        io.bhex.base.account.SetOrderSettingRequest req = io.bhex.base.account.SetOrderSettingRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setAccountId(accountService.getFuturesAccountId(request.getHeader()))
                .setIsPassiveOrder(request.getIsPassiveOrder())
                .setIsConfirm(request.getIsConfirm())
                .setTimeInForce(request.getTimeInForce())
                .build();
        io.bhex.base.account.SetOrderSettingResponse response = grpcFuturesOrderService.setOrderSetting(req);
        return SetOrderSettingResponse.newBuilder()
                .setSuccess(response.getSuccess())
                .build();
    }

    public GetRiskLimitResponse getRiskLimit(GetRiskLimitRequest request) {
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                request.getHeader().getOrgId(), request.getSymbolId());
        if (symbolDetail == null) {
            log.warn("getRiskLimit failed for invalid symbolId: {} orgId: {}",
                    request.getSymbolId(), request.getHeader().getOrgId());
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        Long futuresAccountId = accountService.getFuturesIndexAccountId(request.getHeader(), request.getAccountIndex());
        io.bhex.base.account.GetRiskLimitRequest req = io.bhex.base.account.GetRiskLimitRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(request.getHeader().getOrgId()))
                .setAccountId(futuresAccountId)
                .setSymbolId(request.getSymbolId())
                .build();
        io.bhex.base.account.GetRiskLimitResponse response = grpcFuturesOrderService.getRiskLimit(req);
        return GetRiskLimitResponse.newBuilder()
                .addAllRiskLimits(FuturesUtil.toRiskLimits(response.getRiskLimitsList(), symbolDetail))
                .setAccountIndex(request.getAccountIndex())
                .setAccountType(AccountTypeEnum.FUTURE)
                .build();
    }

    private FuturesRiskLimit getRiskLimitById(Long riskLimitId, Header header, String symbolId) {
        GetRiskLimitRequest request = GetRiskLimitRequest.newBuilder()
                .setHeader(header)
                .setSymbolId(symbolId)
                .build();

        GetRiskLimitResponse response = getRiskLimit(request);
        List<FuturesRiskLimit> riskLimitsList = response.getRiskLimitsList();
        for (FuturesRiskLimit r : riskLimitsList) {
            if (riskLimitId.equals(r.getRiskLimitId())) {
                return r;
            }
        }
        return null;
    }

    public GetOrderSettingResponse getOrderSetting(GetOrderSettingRequest request) {
        Header header = request.getHeader();
        io.bhex.base.account.GetOrderSettingRequest req = io.bhex.base.account.GetOrderSettingRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountService.getFuturesAccountId(header))
                .build();
        io.bhex.base.account.GetOrderSettingResponse response = grpcFuturesOrderService.getOrderSetting(req);
        return GetOrderSettingResponse.newBuilder()
                .setOrderSetting(FuturesUtil.toOrderSetting(response.getOrderSetting()))
                .build();
    }

    public GetOrderSettingResponse getOrderSetting(Header header) {
        io.bhex.base.account.GetOrderSettingRequest req = io.bhex.base.account.GetOrderSettingRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountService.getFuturesAccountId(header))
                .build();
        io.bhex.base.account.GetOrderSettingResponse response = grpcFuturesOrderService.getOrderSetting(req);
        return GetOrderSettingResponse.newBuilder()
                .setOrderSetting(FuturesUtil.toOrderSetting(response.getOrderSetting()))
                .build();
    }

    public OrgFuturesPositionsResponse getOrgFuturesPositions(OrgFuturesPositionsRequest request) {
        io.bhex.base.account.FuturesPositionsRequest req = io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setOrgId(request.getHeader().getOrgId())
                .addAllTokenIds(request.getTokenIdsList())
                .setFromBalanceId(request.getFromBalanceId())
                .setEndBalanceId(request.getEndBalanceId())
                .setLimit(request.getLimit())
                .setIsLong(-1)
                .build();

        io.bhex.base.account.FuturesPositionsResponse res = grpcFuturesOrderService.getFuturesPositions(req);
        List<OrgFuturesPosition> orgFuturesPositions =
                Optional.ofNullable(res.getPositionsList()).orElse(new ArrayList<>())
                        .stream()
                        .map(position -> toOrgPosition(request.getHeader(), position))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        return OrgFuturesPositionsResponse.newBuilder().addAllPositions(orgFuturesPositions).build();
    }

    public FuturesPositionsResponse getFuturesPositions(FuturesPositionsRequest request) {
        if (!BrokerService.checkModule(request.getHeader(), FunctionModule.FUTURES)) {
            return FuturesPositionsResponse.getDefaultInstance();
        }
        Header header = request.getHeader();

        Long accountId = accountService.getFuturesIndexAccountId(header, request.getAccountIndex());
        io.bhex.base.account.FuturesPositionsRequest req = io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setAccountId(accountId)
                .addAllTokenIds(request.getTokenIdsList())
                .setFromBalanceId(request.getFromBalanceId())
                .setEndBalanceId(request.getEndBalanceId())
                .setLimit(request.getLimit())
                .setIsLong(-1)
                .setIsTotalPositive(true)
                .build();

        List<FuturesPosition> result = getBhexFuturesPositionsEx(request.getHeader(), req, accountId);
        return FuturesPositionsResponse.newBuilder()
                .addAllPositions(result)
                .build();
    }

    private FuturesPosition toPositionEx(Header header, io.bhex.base.account.FuturesPosition position, Long futuresAccountId) {
        TokenFuturesInfo info = basicService.getTokenFuturesInfoMap().get(position.getTokenId());
        if (info == null) {
            log.warn("tokenFuturesInfo error. tokenId:{}", position.getTokenId());
            return null;
        }
        BalanceDetail balance = assetFuturesService.queryBalanceDetail(futuresAccountId, info.getCoinToken(), header.getOrgId());
        BigDecimal coinAvailable = (balance != null ? DecimalUtil.toBigDecimal(balance.getAvailable()) : BigDecimal.ZERO);
        return toPosition(header, position, coinAvailable, null, null);
    }

    private FuturesPosition toPosition(Header header, io.bhex.base.account.FuturesPosition position) {
        return toPosition(header, position, null, null, null);
    }

    public QueryLiquidationPositionResponse getLiquidationPosition(Header header, Long accountId, Long orderId) {
        GetLiquidationPositionRequest request = GetLiquidationPositionRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setOrderId(orderId)
                .build();

        GetLiquidationPositionResponse response = grpcFuturesOrderService.getLiquidationPosition(request);
        BigDecimal indexPrice = DecimalUtil.toBigDecimal(response.getIndexPrice());
        SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(response.getPosition().getExchangeId(),
                response.getPosition().getTokenId());
        if (symbolDetail == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        // 如果是反向合约 平台返回的指数价格要取倒数
        if (symbolDetail.getIsReverse() && indexPrice.compareTo(BigDecimal.ZERO) > 0) {
            indexPrice = BigDecimal.ONE.divide(indexPrice, RoundingMode.DOWN);
        }

        FuturesPosition futuresPosition = toPosition(header, response.getPosition(), null, indexPrice, indexPrice);
        return QueryLiquidationPositionResponse.newBuilder().setFuturesPosition(futuresPosition).build();
    }

    /**
     * 获取期货持仓信息
     *
     * @param header   request header
     * @param position 当前期货持仓
     * @return 持仓信息
     */
    private FuturesPosition toPosition(Header header, io.bhex.base.account.FuturesPosition position, BigDecimal coinAvailable, BigDecimal currentPrice, BigDecimal indexPrice) {
        String key = String.format("%s_%s", header.getOrgId(), position.getTokenId());
        Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
        if (exchangeId == null) {
            log.warn("toPosition get exchangeId null. tokenId:{}, orgId:{}", position.getTokenId(), header.getOrgId());
            return null;
        }

        SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, position.getTokenId());
        if (symbolDetail == null) {
            log.warn("toPosition get symbolDetail null. tokenId:{}, exchangeId:{}", position.getTokenId(), exchangeId);
            return null;
        }

        // 基本数量精度
        DecimalStringFormatter baseFmt = new DecimalStringFormatter(symbolDetail.getBasePrecision());
        // 保证金精度
        DecimalStringFormatter marginFmt = new DecimalStringFormatter(symbolDetail.getMarginPrecision());

        if (currentPrice == null) {
            currentPrice = futuresQuoteService.getCurrentPrice(symbolDetail.getSymbolId(), exchangeId, header.getOrgId());
        }
        TokenFuturesInfo tokenFuturesInfo = basicService.getTokenFuturesInfoMap().get(position.getTokenId());

        if (currentPrice == null || tokenFuturesInfo == null) {
            log.warn("toPosition error. tokenId:{}, exchangeId:{}", position.getTokenId(), exchangeId);
            return null;
        }

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        // 价格精度
        DecimalStringFormatter minPriceFmt = new DecimalStringFormatter(minPricePrecision);
        BigDecimal contractMultiplier = DecimalUtil.toBigDecimal(tokenFuturesInfo.getContractMultiplier());
        BigDecimal avgPrice = FuturesUtil.div(DecimalUtil.toBigDecimal(position.getOpenValue()), DecimalUtil.toBigDecimal(position.getTotal()).multiply(contractMultiplier));

        BigDecimal positionValue;
        BigDecimal marginRate;
        BigDecimal leverage;
        BigDecimal unRealisedPnl;

        if (!tokenFuturesInfo.getIsReverse()) {
            unRealisedPnl = FuturesUtil.getUnRealisedPnl(
                    DecimalUtil.toBigDecimal(position.getOpenValue()),
                    currentPrice,
                    contractMultiplier,
                    DecimalUtil.toBigDecimal(position.getTotal()),
                    position.getIsLong()
            );

            positionValue = DecimalUtil.toBigDecimal(position.getTotal()).multiply(currentPrice).multiply(contractMultiplier);
            marginRate = FuturesUtil.div(DecimalUtil.toBigDecimal(position.getMargin()).add(unRealisedPnl), positionValue);
            leverage = FuturesUtil.div(BigDecimal.ONE, marginRate);


        } else {
            unRealisedPnl = FuturesUtil.getUnRealisedPnl(
                    DecimalUtil.toBigDecimal(position.getOpenValue()),
                    FuturesUtil.div(BigDecimal.ONE, currentPrice),
                    contractMultiplier,
                    DecimalUtil.toBigDecimal(position.getTotal()),
                    position.getIsLong()
            );

            positionValue = DecimalUtil.toBigDecimal(position.getTotal()).multiply(FuturesUtil.div(BigDecimal.ONE, currentPrice)).multiply(contractMultiplier);
            marginRate = FuturesUtil.div(DecimalUtil.toBigDecimal(position.getMargin()).add(unRealisedPnl), positionValue);
            leverage = FuturesUtil.div(BigDecimal.ONE, marginRate);
        }

        String indices;
        if (indexPrice == null) {
            indices = minPriceFmt.format(futuresQuoteService.getIndices(symbolDetail.getDisplayIndexToken(), header.getOrgId()));
        } else {
            indices = minPriceFmt.format(indexPrice);
        }

        String avgPriceVal;
        String liquidationPriceVal;
        if (avgPrice.compareTo(BigDecimal.ZERO) > 0) {
            avgPriceVal = symbolDetail.getIsReverse() ? minPriceFmt.reciprocalFormat(avgPrice) : minPriceFmt.format(avgPrice);
        } else {
            avgPriceVal = minPriceFmt.format(avgPrice);
        }
        if (DecimalUtil.toBigDecimal(position.getLiquidationPrice()).compareTo(BigDecimal.ZERO) > 0) {
            liquidationPriceVal = symbolDetail.getIsReverse() ?
                    minPriceFmt.reciprocalFormat(position.getLiquidationPrice()) :
                    minPriceFmt.format(position.getLiquidationPrice());
        } else {
            liquidationPriceVal = minPriceFmt.format(position.getLiquidationPrice());
        }

//        log.info("toPosition:userId {},accountId {},positionId {},symbolId {},contractMultiplier {},avgPriceVal {},positionValue {},marginRate {},leverage {},unRealisedPnl {},indices {},liquidationPriceVal {}, currentPrice {},total {}",
//                header.getUserId(),
//                position.getAccountId(),
//                position.getPositionId(),
//                symbolDetail.getSymbolId(),
//                contractMultiplier,
//                avgPriceVal,
//                positionValue.toPlainString(),
//                marginRate.toPlainString(),
//                leverage,
//                unRealisedPnl.toPlainString(),
//                indices,
//                liquidationPriceVal,
//                currentPrice,
//                position.getTotal());
        FuturesRiskLimit riskLimit = getRiskLimitById(position.getRiskLimitId(), header, position.getTokenId());
        BigDecimal minMargin = BigDecimal.ZERO;
        if (Objects.nonNull(riskLimit)) {
            // minMargin = getMinMargin(marginRate, new BigDecimal(riskLimit.getInitialMargin()), positionValue, DecimalUtil.toBigDecimal(position.getMargin()));
            // 最大可减少保证金：margin - 开仓价值 * 对应风险限额档位的起始保证金率
            minMargin = DecimalUtil.toBigDecimal(position.getMargin())
                    .subtract(
                            DecimalUtil.toBigDecimal(position.getOpenValue())
                                    .multiply(new BigDecimal(riskLimit.getInitialMargin())));
            if (minMargin.compareTo(BigDecimal.ZERO) < 0) {
                // 最大可减少保证金可能是负值，这里需要处理成0
                minMargin = BigDecimal.ZERO;
            }
        }

        // 仓位方向: 1=多仓，0=空仓
        String isLong = symbolDetail.getIsReverse() ? (position.getIsLong() ? "0" : "1") : (position.getIsLong() ? "1" : "0");

        DecimalStringFormatter leverageFmt = new DecimalStringFormatter(FUTURES_LEVERAGE_PRECISION);
        DecimalStringFormatter marginRateFmt = new DecimalStringFormatter(FUTURES_MARGIN_RATE_PRECISION);

        FuturesPosition.Builder builder = FuturesPosition.newBuilder()
                .setPositionId(position.getPositionId()) //position id
                .setAccountId(position.getAccountId())//账户id
                .setTokenId(position.getTokenId())//期货token id
                .setLeverage(leverageFmt.format(leverage))//扛杆
                .setTotal(baseFmt.format(position.getTotal()))//仓位
                .setPositionValues(marginFmt.format(positionValue))///仓位价值（USDT）
                .setMargin(marginFmt.format(position.getMargin()))//仓位保证金（USDT）
                .setOrderMargin(marginFmt.format(position.getOrderMargin()))//委托保证金（USDT）
                .setAvgPrice(avgPriceVal)//开仓均价
                .setLiquidationPrice(liquidationPriceVal)//预估强平价
                .setMarginRate(marginRateFmt.format(marginRate)) //保证金率
                .setIndices(indices)//交割指数
                .setAvailable(baseFmt.format(position.getAvailable()))//可平量
                .setIsLong(isLong) // 仓位方向
                .setRealisedPnl(marginFmt.format(position.getRealisedPnl()))//已实现盈亏
                .setUnrealisedPnl(marginFmt.format(unRealisedPnl))//未实现盈亏
                .setMinMargin(marginFmt.format(minMargin))
                .setOpenOnBook(marginFmt.format(position.getOpenOnBook()));

        // 持仓收益率
        String profitRate = getProfitRate(unRealisedPnl, DecimalUtil.toBigDecimal(position.getMargin()));
        builder.setProfitRate(profitRate);

        if (coinAvailable != null) {
            builder.setCoinAvailable(marginFmt.format(coinAvailable));
        }

        return builder.build();
    }

    private OrgFuturesPosition toOrgPosition(Header header, io.bhex.base.account.FuturesPosition position) {
        String key = String.format("%s_%s", header.getOrgId(), position.getTokenId());
        Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
        if (exchangeId == null) {
            log.warn("toOrgPosition get exchangeId null. tokenId:{}, orgId:{}", position.getTokenId(), header.getOrgId());
            return null;
        }

        SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, position.getTokenId());
        if (symbolDetail == null) {
            log.warn("toOrgPosition get symbolDetail null. tokenId:{}, exchangeId:{}", position.getTokenId(), exchangeId);
            return null;
        }

        // 仓位方向: 1=多仓，0=空仓
        String isLong = symbolDetail.getIsReverse() ? (position.getIsLong() ? "0" : "1") : (position.getIsLong() ? "1" : "0");

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        // 价格精度
        DecimalStringFormatter minPriceFmt = new DecimalStringFormatter(minPricePrecision);

        // 基本数量精度
        DecimalStringFormatter baseFmt = new DecimalStringFormatter(symbolDetail.getBasePrecision());
        // 保证金精度
        DecimalStringFormatter marginFmt = new DecimalStringFormatter(symbolDetail.getMarginPrecision());

        String bankruptcyPriceVal;
        String liquidationPriceVal;

        if (DecimalUtil.toBigDecimal(position.getLiquidationPrice()).compareTo(BigDecimal.ZERO) > 0) {
            liquidationPriceVal = symbolDetail.getIsReverse() ?
                    minPriceFmt.reciprocalFormat(position.getLiquidationPrice()) :
                    minPriceFmt.format(position.getLiquidationPrice());
        } else {
            liquidationPriceVal = minPriceFmt.format(position.getLiquidationPrice());
        }

        if (DecimalUtil.toBigDecimal(position.getBankruptcyPrice()).compareTo(BigDecimal.ZERO) > 0) {
            bankruptcyPriceVal = symbolDetail.getIsReverse() ?
                    minPriceFmt.reciprocalFormat(position.getBankruptcyPrice()) :
                    minPriceFmt.format(position.getLiquidationPrice());
        } else {
            bankruptcyPriceVal = minPriceFmt.format(position.getBankruptcyPrice());
        }

        return OrgFuturesPosition.newBuilder()
                .setPositionId(position.getPositionId())
                .setAccountId(position.getAccountId())
                .setUserId(position.getBrokerUserId())
                .setSymbolId(position.getTokenId())
                .setTotal(baseFmt.format(position.getTotal()))
                .setLocked(baseFmt.format(position.getLocked()))
                .setAvailable(baseFmt.format(position.getAvailable()))
                .setMargin(marginFmt.format(position.getMargin()))
                .setOrderMargin(marginFmt.format(position.getOrderMargin()))
                .setOpenValue(marginFmt.format(position.getOpenValue()))
                .setRealisedPnl(marginFmt.format(position.getRealisedPnl()))
                .setRiskLimitId(position.getRiskLimitId())
                .setOpenOnBook(marginFmt.format(position.getOpenOnBook()))
                .setLiquidationPrice(liquidationPriceVal)
                .setBankruptcyPrice(bankruptcyPriceVal)
                .setIsLong(isLong)
                .build();
    }

    /**
     * 计算持仓收益率
     */
    private static String getProfitRate(BigDecimal unRealisedPnl, BigDecimal margin) {
        if (unRealisedPnl.compareTo(BigDecimal.ZERO) == 0 || margin.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO.toPlainString();
        }

        return unRealisedPnl.divide(margin, 18, RoundingMode.DOWN)
                .setScale(4, RoundingMode.DOWN)
                .stripTrailingZeros()
                .toPlainString();
    }

    /**
     * if 保证金率 > 当前风险限额初始保证金率： = min{（保证金率 - 当前风险限额初始保证金率）* 仓位价值, ( 起始保证金 + 累计追加或减少保证金 ) } else： =
     * 0
     */
    private BigDecimal getMinMargin(BigDecimal marginRate, BigDecimal initialMarginRate, BigDecimal positionValue, BigDecimal margin) {
        if (marginRate.compareTo(initialMarginRate) > 0) {
            BigDecimal minMargin = marginRate.subtract(initialMarginRate).multiply(positionValue);
            if (minMargin.compareTo(margin) > 0) {
                return margin;
            } else {
                return minMargin;
            }
        } else {
            return BigDecimal.ZERO;
        }

    }

    public FuturesSettlementResponse getFuturesSettlement(Header header, String side,
                                                          long fromSettlementId, long endSettlementId, long startTime, long endTime, int limit) {
        Long accountId = accountService.getFuturesAccountId(header);
        if (accountId == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        OptionSettlementReq settlementReq = OptionSettlementReq
                .newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setSide(side)
                .setFromSettlementId(fromSettlementId)
                .setEndSettlementId(endSettlementId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit)
                .build();
        OptionSettlementReply reply = grpcFuturesOrderService.getFuturesSettlement(settlementReq);
        List<FuturesSettlement> settlements = new ArrayList<>();
        reply.getOptionSettlementList().forEach(s -> {
            ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
            Symbol symbol = optionSymbolMap.get(header.getOrgId() + "_" + s.getTokenId());
            if (symbol != null) {
                SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                settlements.add(FuturesUtil.buildSettlement(s, symbol, symbolDetail));
            }
        });
        return FuturesSettlementResponse.newBuilder().addAllFuturesSettlement(settlements).build();
    }

    public GetOrderResponse getFuturesOrderInfo(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex, Long orderId, String clientOrderId) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getFuturesAccountId(header);
        Long accountId = accountService.getFuturesIndexAccountId(header, accountIndex);
        io.bhex.base.account.GetOrderRequest request = GetOrderRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setOrderId(orderId)
                .setClientOrderId(clientOrderId)
                .build();
        GetOrderReply reply = grpcFuturesOrderService.getFuturesOrderInfo(request);
        String symbolId = reply.getOrder().getSymbolId();

        String key = String.format("%s_%s", header.getOrgId(), symbolId);
        Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
        SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, symbolId);
        if (symbolDetail == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        return GetOrderResponse.newBuilder().setOrder(getFuturesOrder(reply.getOrder(), symbolDetail, exchangeId)).build();
    }

    public Order getFuturesOrder(io.bhex.base.account.Order order, SymbolDetail symbolDetail, Long exchangeId) {
        String statusCode = order.getStatus().name();
        List<Fee> fees;

        // 手续费的显示精度使用保证金的精度
        int marginPrecision = DecimalUtil.toBigDecimal(symbolDetail.getMarginPrecision()).stripTrailingZeros().scale();
        DecimalStringFormatter feeFormatter = new DecimalStringFormatter(marginPrecision, BigDecimal.ROUND_DOWN, true);

        // 保证金
        DecimalStringFormatter marginFormatter = feeFormatter;
        // TODO: 目前平台的Order没有返回下单时的委托保证金，只返回了锁定的保证金 需要更新bhex的proto来增加margin字段
        String marginLocked = marginFormatter.format(order.getOrderMarginLocked());

        //判断是做多还是做空
        if (order.getSide().name().equals(OrderSide.BUY.name())) {
            fees = order.getTradeFeesList().stream()
                    .map(fee -> Fee.newBuilder()
                            .setFeeTokenId(fee.getToken().getTokenId())
                            .setFeeTokenName(order.getSymbol().getQuoteToken().getTokenName())
                            .setFee(feeFormatter.format(fee.getFee()))
                            .build())
                    .collect(Collectors.toList());
        } else {
            fees = order.getTradeFeesList().stream()
                    .map(fee -> Fee.newBuilder()
                            .setFeeTokenId(fee.getToken().getTokenId())
                            .setFeeTokenName(fee.getToken().getTokenName())
                            .setFee(feeFormatter.format(fee.getFee()))
                            .build())
                    .collect(Collectors.toList());
        }

        OrderType orderType;
        switch (order.getType()) {
            case LIMIT:
                orderType = OrderType.LIMIT;
                break;
            case LIMIT_MAKER:
                orderType = OrderType.LIMIT_MAKER;
                break;
            case LIMIT_FREE:
                orderType = OrderType.LIMIT_FREE;
                break;
            case LIMIT_MAKER_FREE:
                orderType = OrderType.LIMIT_MAKER_FREE;
                break;
            default:
                orderType = OrderType.MARKET;
                break;
        }

        String origQty;
        String executedQty;
        String executedAmountVal;
        String avgPriceVal;
        String priceVal;
        String liquidationPriceVal;

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();
        if (symbolDetail != null) {
            int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
            origQty = order.getType() == OrderTypeEnum.MARKET_OF_QUOTE
                    ? DecimalUtil.toBigDecimal(order.getAmount()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString()
                    : DecimalUtil.toBigDecimal(order.getQuantity()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
            executedQty = DecimalUtil.toBigDecimal(order.getExecutedQuantity()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        } else {
            origQty = DecimalUtil.toTrimString(order.getType() == OrderTypeEnum.MARKET_OF_QUOTE ? order.getAmount() : order.getQuantity());
            executedQty = DecimalUtil.toTrimString(order.getExecutedQuantity());
        }

        String leverage = DecimalUtil.toBigDecimal(order.getLeverage())
                .setScale(FUTURES_LEVERAGE_PRECISION, RoundingMode.DOWN)
                .stripTrailingZeros()
                .toPlainString();

//        if (DecimalUtil.toBigDecimal(order.getExecutedAmount()).compareTo(BigDecimal.ZERO) > 0) {
//            executedAmountVal = symbolDetail.getIsReverse()
//                    ? BigDecimal.ONE.divide(DecimalUtil.toBigDecimal(order.getExecutedAmount()), symbolDetail.getBasePrecision().getScale(), RoundingMode.DOWN).stripTrailingZeros().toPlainString()
//                    : DecimalUtil.toBigDecimal(order.getExecutedAmount()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
//        } else {
//            executedAmountVal = DecimalUtil.toBigDecimal(order.getExecutedAmount()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
//        }
        executedAmountVal = DecimalUtil.toBigDecimal(order.getExecutedAmount()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();

        if (DecimalUtil.toBigDecimal(order.getAveragePrice()).compareTo(BigDecimal.ZERO) > 0) {
            avgPriceVal = symbolDetail.getIsReverse()
                    ? BigDecimal.ONE.divide(DecimalUtil.toBigDecimal(order.getAveragePrice()), minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString()
                    : DecimalUtil.toBigDecimal(order.getAveragePrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        } else {
            avgPriceVal = DecimalUtil.toBigDecimal(order.getAveragePrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        }

        if (DecimalUtil.toBigDecimal(order.getPrice()).compareTo(BigDecimal.ZERO) > 0) {
            priceVal = symbolDetail.getIsReverse()
                    ? BigDecimal.ONE.divide(DecimalUtil.toBigDecimal(order.getPrice()), minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString()
                    : DecimalUtil.toBigDecimal(order.getPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        } else {
            priceVal = DecimalUtil.toBigDecimal(order.getPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        }

        if (DecimalUtil.toBigDecimal(order.getLiquidationPrice()).compareTo(BigDecimal.ZERO) > 0) {
            liquidationPriceVal = symbolDetail.getIsReverse()
                    ? BigDecimal.ONE.divide(DecimalUtil.toBigDecimal(order.getLiquidationPrice()), minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString()
                    : DecimalUtil.toBigDecimal(order.getLiquidationPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        } else {
            liquidationPriceVal = DecimalUtil.toBigDecimal(order.getLiquidationPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        }

        //如果是反向转换方向
        FuturesOrderSide futuresOrderSide = symbolDetail.getIsReverse() ?
                FuturesUtil.getReverseFuturesSide(FuturesUtil.toFuturesOrderSide(order.getSide(), order.getIsClose())) :
                FuturesUtil.toFuturesOrderSide(order.getSide(), order.getIsClose());
        return Order.newBuilder()
                .setExchangeId(Optional.ofNullable(exchangeId).orElse(0L))
                .setAccountId(order.getAccountId())
                .setOrderId(order.getOrderId())
                .setClientOrderId(order.getClientOrderId())
                .setSymbolId(order.getSymbol().getSymbolId())
                .setSymbolName(order.getSymbol().getSymbolName())
                .setBaseTokenId(order.getSymbol().getBaseToken().getTokenId())
                .setBaseTokenName(order.getSymbol().getBaseToken().getTokenName())
                .setQuoteTokenId(order.getSymbol().getQuoteToken().getTokenId())
                .setQuoteTokenName(order.getSymbol().getQuoteToken().getTokenName())
                .setPrice(priceVal)
                .setOrigQty(origQty)
                .setExecutedQty(executedQty)
                .setExecutedAmount(executedAmountVal)
                .setAvgPrice(avgPriceVal)
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(order.getSide().name()))
                .addAllFees(fees)
                .setStatusCode(statusCode)
                .setTime(order.getCreatedTime())
                .setTimeInForce(io.bhex.broker.grpc.order.OrderTimeInForceEnum.valueOf(order.getTimeInForce().name()))
                .setLeverage(leverage)
                .setIsClose(order.getIsClose())
                .setFuturesOrderSide(Order.FuturesOrderSide.forNumber(futuresOrderSide.getNumber()))
                .setOrderMarginLocked(marginLocked)
                .setFuturesPriceType(getPriceType(order.getExtraFlag()))
                .setIsLiquidationOrder(order.getIsLiquidationOrder())
                .setLiquidationType(getLiquidationType(order.getExtraFlag()))
                .setLiquidationPrice(liquidationPriceVal)
                .setClosePnl("0") // TODO: MOCK
                .setLastUpdated(order.getUpdatedTime())
                .build();
    }

    private Order.LiquidationType getLiquidationType(NewOrderRequest.ExtraFlagEnum extraFlagEnum) {
        switch (extraFlagEnum) {
            case LIQUI_IOC_ORDER:
                return Order.LiquidationType.IOC;
            case LIQUI_ADL_ORDER:
                return Order.LiquidationType.ADL;
            default:
                return Order.LiquidationType.NO_LIQ;
        }
    }

    public QueryFuturesOrdersResponse queryFuturesOrders(QueryFuturesOrdersRequest request) {
        Header header = request.getHeader();
        String symbolId = request.getSymbolId();
        Long fromOrderId = request.getFromId();
        Long endOrderId = request.getEndId();
        Long startTime = request.getStartTime();
        Long endTime = request.getEndTime();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && request.getAccountIndex() != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(request.getAccountType()), request.getAccountIndex())
//                : accountService.getFuturesAccountId(header);
        Long accountId = accountService.getFuturesIndexAccountId(header, request.getAccountIndex());

        String baseTokenId = request.getBaseTokenId();
        String quoteTokenId = request.getQuoteTokenId();

        PlanOrder.FuturesOrderType orderType = request.getOrderType();

        if (orderType == PlanOrder.FuturesOrderType.LIMIT) {
            List<Order> orders = queryFuturesOrders(header, accountId, symbolId, fromOrderId, endOrderId, startTime, endTime, baseTokenId, quoteTokenId,
                    request.getOrderSide(), request.getLimit(), request.getQueryType());
            return QueryFuturesOrdersResponse.newBuilder().addAllOrders(orders).build();
        }

        if (orderType == PlanOrder.FuturesOrderType.STOP) {
            List<PlanOrder> orders = queryFuturesPlanOrders(header, accountId, symbolId, fromOrderId, endOrderId,
                    startTime, endTime, request.getLimit(), request.getQueryType(), request.getPlanOrderTypesList());
            return QueryFuturesOrdersResponse.newBuilder().addAllPlanOrders(orders).build();
        }

        return QueryFuturesOrdersResponse.newBuilder().build();

    }

    private List<Order> queryFuturesOrders(Header header, Long accountId, String symbolId, Long fromOrderId, Long endOrderId, Long startTime, Long endTime,
                                           String baseTokenId, String quoteTokenId, OrderSide orderSide, Integer limit, OrderQueryType queryType) {
        List<OrderStatusEnum> orderStatusList = Lists.newArrayList();
        switch (queryType) {
            case ALL:
                orderStatusList = Lists.newArrayList(OrderStatusEnum.NEW, OrderStatusEnum.PARTIALLY_FILLED, OrderStatusEnum.PENDING_CANCEL,
                        OrderStatusEnum.CANCELED, OrderStatusEnum.FILLED);
                break;
            case CURRENT:
                orderStatusList = Lists.newArrayList(OrderStatusEnum.NEW, OrderStatusEnum.PARTIALLY_FILLED, OrderStatusEnum.PENDING_CANCEL);
                break;
            case HISTORY:
                orderStatusList = Lists.newArrayList(OrderStatusEnum.CANCELED, OrderStatusEnum.FILLED);
                break;
            default:
                break;
        }
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == null || startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        GetOrdersRequest.Builder builder = GetOrdersRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setSymbolId(Strings.nullToEmpty(symbolId))
                .setOrderId(fromOrderId)
                .setEndOrderId(endOrderId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setBaseTokenId(Strings.nullToEmpty(baseTokenId))
                .setQuoteTokenId(Strings.nullToEmpty(quoteTokenId))
                .setLimit(limit)
                .addAllOrderStatusList(orderStatusList);

        if (orderSide != null && orderSide != OrderSide.UNKNOWN_ORDER_SIDE) {
            builder.addSide(OrderSideEnum.valueOf(orderSide.name()));
        } else {
            builder.addSide(OrderSideEnum.BUY).addSide(OrderSideEnum.SELL);
        }
        GetOrdersReply reply = grpcFuturesOrderService.queryFuturesOrders(builder.build());
        List<Order> orderList = Optional.ofNullable(reply.getOrdersList())
                .orElse(new ArrayList<>())
                .stream()
                .map(order -> {
                    String key = String.format("%s_%s", header.getOrgId(), order.getSymbolId());
                    Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
                    if (exchangeId == null) {
                        log.warn("getOrgExchangeFuturesMap null. key:{}", key);
                        return null;
                    }
                    SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, order.getSymbolId());
                    if (symbolDetail == null) {
                        log.warn("getSymbolDetailFutures null. exchangeId:{}, symbolId:{}", exchangeId, symbolId);
                        return null;
                    }
                    return getFuturesOrder(order, symbolDetail, exchangeId);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (fromOrderId == 0 && endOrderId > 0 && header.getPlatform() != Platform.OPENAPI) {
            return Lists.reverse(orderList);
        }
        return orderList;
    }

    private List<PlanOrder> queryFuturesPlanOrders(Header header, Long accountId,
                                                   String symbolId, Long fromOrderId, Long endOrderId, Long startTime, Long endTime,
                                                   Integer limit, OrderQueryType queryType, List<PlanOrder.PlanOrderTypeEnum> planOrderTypes) {
        List<io.bhex.base.account.PlanOrder.PlanOrderStatusEnum> statusList = Lists.newArrayList();
        switch (queryType) {
            case ALL:
                statusList = Lists.newArrayList(
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_NEW,
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_FILLED,
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_REJECTED,
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_CANCELED,
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_FAILED
                );
                break;
            case CURRENT:
                statusList = Lists.newArrayList(
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_NEW
                );
                break;
            case HISTORY:
                statusList = Lists.newArrayList(
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_FILLED,
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_REJECTED,
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_CANCELED,
                        io.bhex.base.account.PlanOrder.PlanOrderStatusEnum.ORDER_FAILED
                );
                break;
            default:
                break;
        }

        List<io.bhex.base.account.PlanOrderTypeEnum> bhPlanOrderTypes = new ArrayList<>();
        if (CollectionUtils.isEmpty(planOrderTypes)) {
            // 默认查询普通计划订单
            bhPlanOrderTypes.add(PlanOrderTypeEnum.STOP_COMMON);
        } else {
            planOrderTypes.forEach(e -> bhPlanOrderTypes.add(io.bhex.base.account.PlanOrderTypeEnum.forNumber(e.getNumber() - 1)));
        }
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == null || startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        io.bhex.base.account.GetPlanOrdersRequest req = io.bhex.base.account.GetPlanOrdersRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setSymbolId(symbolId)
                .setFromOrderId(fromOrderId)
                .setEndOrderId(endOrderId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit)
                .addAllOrderStatus(statusList)
                .addAllOrderTypes(bhPlanOrderTypes)
                .build();
        return grpcFuturesOrderService.getPlanOrders(req)
                .getOrdersList()
                .stream()
                .map(planOrder -> {
                    Long exchangeId = basicService.getOrgExchangeFuturesMap().get(String.format("%s_%s", planOrder.getOrgId(), planOrder.getSymbolId()));
                    SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, planOrder.getSymbolId());
                    if (symbolDetail == null) {
                        log.warn("getSymbolDetailFutures null. orgId:{}, symbolId:{}", planOrder.getOrgId(), planOrder.getSymbolId());
                        return null;
                    }
                    return FuturesUtil.toPlanOrder(planOrder, symbolDetail);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private MatchInfo getMatchInfo(Trade trade, SymbolDetail symbolDetail) {
        OrderType orderType;
        switch (trade.getOrderType()) {
            case LIMIT_MAKER:
                orderType = OrderType.LIMIT_MAKER;
                break;
            case LIMIT:
                orderType = OrderType.LIMIT;
                break;
            case LIMIT_FREE:
                orderType = OrderType.LIMIT_FREE;
                break;
            case LIMIT_MAKER_FREE:
                orderType = OrderType.LIMIT_MAKER_FREE;
                break;
            default:
                orderType = OrderType.MARKET;
                break;
        }


        // 手续费的显示精度使用保证金的精度
        int marginPrecision = DecimalUtil.toBigDecimal(symbolDetail.getMarginPrecision()).stripTrailingZeros().scale();
        DecimalStringFormatter feeFormatter = new DecimalStringFormatter(marginPrecision, BigDecimal.ROUND_DOWN, true);
        Fee fee = Fee.newBuilder()
                .setFeeTokenId(trade.getTradeFee().getToken().getTokenId())
                .setFeeTokenName(trade.getTradeFee().getToken().getTokenName())
                .setFee(feeFormatter.format(trade.getTradeFee().getFee()))
                .build();

//        Fee fee;
//        if (trade.getSide().name().equals(OrderSide.BUY.name())) {
//            fee = Fee.newBuilder()
//                    .setFeeTokenId(trade.getSymbol().getQuoteToken().getTokenId())
//                    .setFeeTokenName(trade.getSymbol().getQuoteToken().getTokenName())
//                    .setFee(feeFormatter.format(trade.getTradeFee().getFee()))
//                    .build();
//        } else {
//            fee = Fee.newBuilder()
//                    .setFeeTokenId(trade.getTradeFee().getToken().getTokenId())
//                    .setFeeTokenName(trade.getTradeFee().getToken().getTokenName())
//                    .setFee(feeFormatter.format(trade.getTradeFee().getFee()))
//                    .build();
//        }

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();

        // 根据extra_info获取强平类型
        Order.LiquidationType liquidationType = Order.LiquidationType.NO_LIQ;
        ExtraFlag extraFlag = ExtraFlag.forNumber(trade.getExtraInfo());
        if (extraFlag == null) {
            log.error(String.format("Unknow extraFlag. trade: %s", TextFormat.shortDebugString(trade)));
        } else {
            switch (extraFlag) {
                case LIQUIDATION_TRADE:
                    liquidationType = Order.LiquidationType.IOC;
                    break;
                case LIQUIDATION_ADL:
                    liquidationType = Order.LiquidationType.ADL;
                    break;
            }
        }
        //如果是反向转换方向
        OrderSide orderSide = symbolDetail.getIsReverse() ?
                FuturesUtil.getReverseOrderSide(trade.getSide()) :
                FuturesUtil.getOrderSide(trade.getSide());
        return MatchInfo.newBuilder()
                .setAccountId(trade.getAccountId())
                .setOrderId(trade.getOrderId())
                .setMatchOrderId(trade.getMatchOrderId())
                .setTradeId(trade.getTradeId())
                .setSymbolId(trade.getSymbol().getSymbolId())
                .setSymbolName(trade.getSymbol().getSymbolName())
                .setBaseTokenId(trade.getSymbol().getBaseToken().getTokenId())
                .setBaseTokenName(trade.getSymbol().getBaseToken().getTokenName())
                .setQuoteTokenId(trade.getSymbol().getQuoteToken().getTokenId())
                .setQuoteTokenName(trade.getSymbol().getQuoteToken().getTokenName())
                .setPrice(symbolDetail.getIsReverse() && DecimalUtil.toBigDecimal(trade.getPrice()).compareTo(BigDecimal.ZERO) != 0 ?
                        BigDecimal.ONE.divide(DecimalUtil.toBigDecimal(trade.getPrice()), minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString() :
                        DecimalUtil.toBigDecimal(trade.getPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setQuantity(DecimalUtil.toBigDecimal(trade.getQuantity()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAmount(DecimalUtil.toBigDecimal(trade.getAmount()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setFee(fee)
                .setOrderType(orderType)
                .setOrderSide(orderSide)
                .setMatchOrgId(trade.getOrgId() == trade.getMatchOrgId() ? trade.getMatchOrgId() : 0)
                .setMatchUserId(trade.getOrgId() == trade.getMatchOrgId() ? Long.parseLong(trade.getMatchBrokerUserId()) : 0)
                .setMatchAccountId(trade.getMatchAccountId())
                .setTime(trade.getMatchTime())
                .setIsMaker(trade.getIsMaker())
                .setIsClose(trade.getIsClose())
                .setFuturesPriceType(getPriceType(trade.getExtraFlag()))
                .setPnl(feeFormatter.format(trade.getPnl())) // 盈亏的数值精度使用保证金的精度
                .setLiquidationType(liquidationType)
                .build();
    }

    private MatchInfo getMatchInfo(TradeDetail trade, SymbolDetail symbolDetail) {
        OrderType orderType;
        switch (trade.getOrderType()) {
            case 4:
                orderType = OrderType.LIMIT_MAKER;
                break;
            case 0:
                orderType = OrderType.LIMIT;
                break;
            case 8:
                orderType = OrderType.LIMIT_FREE;
                break;
            case 9:
                orderType = OrderType.LIMIT_MAKER_FREE;
                break;
            default:
                orderType = OrderType.MARKET;
                break;
        }

        // 手续费的显示精度使用保证金的精度
        int marginPrecision = DecimalUtil.toBigDecimal(symbolDetail.getMarginPrecision()).stripTrailingZeros().scale();
        DecimalStringFormatter feeFormatter = new DecimalStringFormatter(marginPrecision, BigDecimal.ROUND_DOWN, true);

        Fee fee = Fee.newBuilder()
                .setFeeTokenId(trade.getFeeToken())
                .setFeeTokenName(basicService.getTokenName(trade.getOrgId(), trade.getFeeToken()))
                .setFee(feeFormatter.format(trade.getFee()))
                .build();

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();

        // 根据extra_info获取强平类型
        Order.LiquidationType liquidationType = Order.LiquidationType.NO_LIQ;
        ExtraFlag extraFlag = ExtraFlag.forNumber(trade.getExtraInfo());
        if (extraFlag == null) {
            log.error(String.format("Unknow extraFlag. trade: %s", JsonUtil.defaultGson().toJson(trade)));
        } else {
            switch (extraFlag) {
                case LIQUIDATION_TRADE:
                    liquidationType = Order.LiquidationType.IOC;
                    break;
                case LIQUIDATION_ADL:
                    liquidationType = Order.LiquidationType.ADL;
                    break;
            }
        }
        //如果是反向转换方向
        OrderSide orderSide = symbolDetail.getIsReverse() ?
                FuturesUtil.getReverseOrderSide(OrderSideEnum.forNumber(trade.getOrderSide())) :
                FuturesUtil.getOrderSide(OrderSideEnum.forNumber(trade.getOrderSide()));
        return MatchInfo.newBuilder()
                .setAccountId(trade.getAccountId())
                .setOrderId(trade.getOrderId())
                .setMatchOrderId(trade.getMatchOrderId())
                .setTradeId(trade.getTradeDetailId())
                .setSymbolId(trade.getSymbolId())
                .setSymbolName(basicService.getSymbolName(trade.getOrgId(), trade.getSymbolId()))
                .setBaseTokenId(trade.getBaseTokenId())
                .setBaseTokenName(trade.getBaseTokenId())
                .setQuoteTokenId(trade.getQuoteTokenId())
                .setQuoteTokenName(basicService.getTokenName(trade.getOrgId(), trade.getQuoteTokenId()))
                .setPrice(symbolDetail.getIsReverse() && trade.getPrice().compareTo(BigDecimal.ZERO) != 0 ?
                        BigDecimal.ONE.divide(trade.getPrice(), minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString() :
                        trade.getPrice().setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setQuantity(trade.getQuantity().setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAmount(trade.getAmount().setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setFee(fee)
                .setOrderType(orderType)
                .setOrderSide(orderSide)
                .setMatchOrgId(trade.getMatchOrgId() != null && trade.getOrgId().equals(trade.getMatchOrgId()) ? trade.getMatchOrgId() : 0)
                .setMatchUserId(trade.getMatchOrgId() != null && trade.getMatchUserId() != null && trade.getOrgId().equals(trade.getMatchOrgId()) ? trade.getMatchUserId() : 0)
                .setMatchAccountId(trade.getMatchAccountId() != null ? trade.getMatchAccountId() : 0)
//                .setTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(trade.getMatchTime(), new ParsePosition(1)).getTime())
                .setTime(trade.getMatchTime().getTime())
                .setIsMaker(trade.getIsMaker() == 1)
                .setIsClose(trade.getIsClose() == 1)
                .setFuturesPriceType(getPriceType(NewOrderRequest.ExtraFlagEnum.forNumber(trade.getExtraInfo())))
                .setPnl(trade.getTradeDetailFutures() == null ? "0" :
                        feeFormatter.format(trade.getTradeDetailFutures().getPnl().add(trade.getTradeDetailFutures().getResidual().negate()))) // 盈亏的数值精度使用保证金的精度
                .setLiquidationType(liquidationType)
                .build();
    }

    public QueryMatchResponse queryFuturesMatchInfo(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex, String symbolId, Long fromTraderId, Long endTradeId,
                                                    Long startTime, Long endTime, Integer limit, OrderSide orderSide, boolean fromEsHistory) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getFuturesAccountId(header);
        Long accountId = accountService.getFuturesIndexAccountId(header, accountIndex);
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == null || startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        GetTradesRequest.Builder builder = GetTradesRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setSymbolId(symbolId)
                .setFromTradeId(fromTraderId)
                .setEndTradeId(endTradeId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit);
        Integer orderSideValue = null;
        if (orderSide != null && !orderSide.name().equals(OrderSide.UNKNOWN_ORDER_SIDE.name())) {
            builder.addSide(OrderSideEnum.valueOf(orderSide.name()));
            orderSideValue = OrderSideEnum.valueOf(orderSide.name()).getNumber();
        } else {
            builder.addSide(OrderSideEnum.BUY).addSide(OrderSideEnum.SELL);
        }
        List<MatchInfo> matchInfoList = Lists.newArrayList();
        if (fromTraderId == 0 && endTradeId > 0) {
            List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, 0L, symbolId, "", orderSideValue, startTime, endTime, fromTraderId, endTradeId, limit, true);
            matchInfoList.addAll(convertEsTradeDetailLists(header, tradeDetailList));
            if (tradeDetailList.size() < limit) {
                Long nextId = endTradeId;
                if (tradeDetailList.size() > 0) {
                    nextId = tradeDetailList.get(tradeDetailList.size() - 1).getTradeDetailId();
                }
                builder.setEndTradeId(nextId).setLimit(limit - tradeDetailList.size());
                GetTradesReply reply = grpcFuturesOrderService.queryFuturesMatchInfo(builder.build());
                matchInfoList.addAll(convertBhTradesList(header, reply.getTradesList()));
            }
        } else {
            GetTradesReply reply = grpcFuturesOrderService.queryFuturesMatchInfo(builder.build());
            matchInfoList.addAll(convertBhTradesList(header, reply.getTradesList()));
            if (reply.getTradesCount() < limit) {
                Long nextId = fromTraderId;
                if (reply.getTradesCount() > 0) {
                    nextId = reply.getTradesList().get(reply.getTradesCount() - 1).getTradeId();
                }
                List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, 0L, symbolId, "", orderSideValue, startTime, endTime,
                        nextId, endTradeId, limit - reply.getTradesCount(), true);
                matchInfoList.addAll(convertEsTradeDetailLists(header, tradeDetailList));
            }
        }
        if (fromTraderId == 0 && endTradeId > 0 && header.getPlatform() != Platform.OPENAPI) {
            return QueryMatchResponse.newBuilder().addAllMatch(Lists.reverse(matchInfoList)).build();
        }
        return QueryMatchResponse.newBuilder().addAllMatch(matchInfoList).build();
    }

    public GetOrderMatchResponse getFuturesOrderMatchInfo(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex, Long orderId, Long fromTraderId, Integer limit) {
        Long accountId = accountService.getFuturesIndexAccountId(header, accountIndex);
        if (orderId <= 0) {
            return GetOrderMatchResponse.getDefaultInstance();
        }
        List<MatchInfo> matchInfoList = Lists.newArrayList();
        List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, orderId, "", "", null,
                0, 0, 0L, 0L, limit, true);
        if (!CollectionUtils.isEmpty(tradeDetailList)) {
            matchInfoList.addAll(convertEsTradeDetailLists(header, tradeDetailList));
        } else {
            GetOrderTradeDetailRequest request = GetOrderTradeDetailRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                    .setAccountId(accountId)
                    .setOrderId(orderId)
                    .setFromTradeId(fromTraderId)
                    .setLimit(limit)
                    .build();
            GetOrderTradeDetailReply reply = grpcFuturesOrderService.getFuturesMatchInfo(request);
            matchInfoList.addAll(convertBhTradesList(header, reply.getTradesList()));
        }
        return GetOrderMatchResponse.newBuilder().addAllMatch(matchInfoList).build();
    }

    private List<MatchInfo> convertBhTradesList(Header header, List<Trade> trades) {
        return Optional.ofNullable(trades)
                .orElse(new ArrayList<>())
                .stream()
                .map(trade -> {
                    String symbolId = trade.getSymbolId();
                    String key = String.format("%s_%s", header.getOrgId(), symbolId);
                    Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
                    if (exchangeId == null) {
                        log.warn("getOrgExchangeFuturesMap null. key:{}", key);
                        return null;
                    }
                    SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, symbolId);
                    if (symbolDetail == null) {
                        log.warn("getSymbolDetailFutures null. exchangeId:{}, symbolId:{}", exchangeId, symbolId);
                        return null;
                    }
                    return getMatchInfo(trade, symbolDetail);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<MatchInfo> convertEsTradeDetailLists(Header header, List<TradeDetail> trades) {
        return Optional.ofNullable(trades)
                .orElse(new ArrayList<>())
                .stream()
                .map(trade -> {
                    String symbolId = trade.getSymbolId();
                    String key = String.format("%s_%s", header.getOrgId(), symbolId);
                    Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
                    if (exchangeId == null) {
                        log.warn("getOrgExchangeFuturesMap null. key:{}", key);
                        return null;
                    }
                    SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, symbolId);
                    if (symbolDetail == null) {
                        log.warn("getSymbolDetailFutures null. exchangeId:{}, symbolId:{}", exchangeId, symbolId);
                        return null;
                    }
                    return getMatchInfo(trade, symbolDetail);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<FuturesPosition> getBhexFuturesPositions(Header header,
                                                          io.bhex.base.account.FuturesPositionsRequest request) {
        io.bhex.base.account.FuturesPositionsResponse res = grpcFuturesOrderService.getFuturesPositions(request);
        return Optional.ofNullable(res.getPositionsList()).orElse(new ArrayList<>())
                .stream()
                .map(position -> toPosition(header, position))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<FuturesPosition> getBhexFuturesPositionsEx(Header header,
                                                            io.bhex.base.account.FuturesPositionsRequest request,
                                                            Long futuresAccountId) {
        io.bhex.base.account.FuturesPositionsResponse res = grpcFuturesOrderService.getFuturesPositions(request);
        return Optional.ofNullable(res.getPositionsList()).orElse(new ArrayList<>())
                .stream()
                .map(position -> toPositionEx(header, position, futuresAccountId))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public GetFuturesTradeAbleResponse getFuturesTradeAble(GetFuturesTradeAbleRequest request) {
        Long futuresAccountId = accountService.getFuturesAccountId(request.getHeader());
        List<String> tokenIds = request.getTokenIdsList();

        io.bhex.base.account.FuturesPositionsRequest bhexRequest = io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setAccountId(futuresAccountId)
                .addAllTokenIds(tokenIds)
                .setIsLong(-1)
                .build();
        List<FuturesPosition> positions = getBhexFuturesPositions(request.getHeader(), bhexRequest);
        List<FuturesTradeAble> tradeAbles = buildTradeAbleList(futuresAccountId, tokenIds, positions, request.getHeader());

        return GetFuturesTradeAbleResponse.newBuilder()
                .addAllFuturesTradeAble(tradeAbles)
                .build();
    }

    /**
     * 同时获取用户期货的可交易信息列表和持仓信息列表
     * <p>
     * 注：此接口是主要用来向前端同时推送FuturesTradeAble和FuturesPosition，因此不支持limit参数，
     * 请求参数主要是用户的期货账户ID，可选参数是tokenId列表
     *
     * @param request GetTradeableAndPositionsRequest
     * @return GetTradeableAndPositionsResponse
     */
    public GetTradeableAndPositionsResponse getTradeableAndPositions(GetTradeableAndPositionsRequest request) {
        Long futuresAccountId = accountService.getFuturesAccountId(request.getHeader());

        io.bhex.base.account.FuturesPositionsRequest bhexRequest = io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setAccountId(futuresAccountId)
                .addAllTokenIds(request.getTokenIdsList())
                .setIsLong(-1)
                .build();

        GetTradeableAndPositionsResponse.Builder responseBuilder = GetTradeableAndPositionsResponse.newBuilder();

        // 构建持仓信息
        List<FuturesPosition> positions = getBhexFuturesPositionsEx(request.getHeader(), bhexRequest, futuresAccountId);
        responseBuilder.addAllPositions(positions);

        // 构建可交易信息
        List<FuturesTradeAble> tradeAbles = buildTradeAbleList(futuresAccountId, request.getTokenIdsList(), positions, request.getHeader());
        responseBuilder.addAllTradeAbles(tradeAbles);

        return responseBuilder.build();
    }

    private Map<String, FuturesPosition> buildPositionMap(List<FuturesPosition> positions) {
        return positions.stream().collect(
                Collectors.toMap(
                        t -> String.format("%s_%s", t.getTokenId(), t.getIsLong()),
                        symbol -> symbol,
                        (p, q) -> p)
        );
    }

    private FuturesTradeAble buildTradeAbleEx(String tokenId,
                                              Long futuresAccountId,
                                              Map<String, FuturesPosition> positionMap,
                                              Header header) {
        TokenFuturesInfo info = basicService.getTokenFuturesInfoMap().get(tokenId);
        if (info == null) {
            log.warn("tokenFuturesInfo error. tokenId:{}", tokenId);
            return null;
        }
        BalanceDetail balance = assetFuturesService.queryBalanceDetail(futuresAccountId, info.getCoinToken(), header.getOrgId());
        BigDecimal coinAvailable = (balance != null ? DecimalUtil.toBigDecimal(balance.getAvailable()) : BigDecimal.ZERO);

        String longKey = String.format("%s_%s", tokenId, FuturesUtil.LONG_VALUE);
        String shortKey = String.format("%s_%s", tokenId, FuturesUtil.SHORT_VALUE);

        FuturesPosition longPosition = positionMap.get(longKey);
        FuturesPosition shortPosition = positionMap.get(shortKey);

        return buildTradeAble(tokenId, coinAvailable, longPosition, shortPosition, header);
    }

    private FuturesTradeAble buildTradeAble(String tokenId,
                                            BigDecimal coinAvailable,
                                            FuturesPosition longPosition,
                                            FuturesPosition shortPosition,
                                            Header header) {
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(header.getOrgId(), tokenId);
        if (symbolDetail == null) {
            log.warn("buildTradeAble get symbolDetail null. orgId: {} tokenId: {}", header.getOrgId(), tokenId);
            return null;
        }

        // 获取保证金显示精度
        DecimalStringFormatter marginFmt = new DecimalStringFormatter(symbolDetail.getMarginPrecision());

        FuturesTradeAble.Builder builder = FuturesTradeAble.newBuilder();
        if (longPosition != null) {
            builder.setLongAvailable(longPosition.getAvailable());
            // 总数量为 当前持仓，以及未完成委托的张数加总。不包含平仓单
            BigDecimal longTotal = new BigDecimal(longPosition.getTotal()).add(new BigDecimal(longPosition.getOpenOnBook()));
            builder.setLongTotal(marginFmt.format(longTotal));
            builder.setLongProfitLoss(getProfitLossResult(longPosition));
        }

        if (shortPosition != null) {
            builder.setShortAvailable(shortPosition.getAvailable());
            // 总数量为 当前持仓，以及未完成委托的张数加总。不包含平仓单
            BigDecimal shortTotal = new BigDecimal(shortPosition.getTotal()).add(new BigDecimal(shortPosition.getOpenOnBook()));
            builder.setShortTotal(marginFmt.format(shortTotal));
            builder.setShortProfitLoss(getProfitLossResult(shortPosition));
        }
        ProfitLossResult profitLossResult = createProfitLossResult(builder.build(), coinAvailable, symbolDetail);

        builder.setTokenId(tokenId);
        builder.setProfitLoss(profitLossResult);
        return builder.build();
    }

    private List<FuturesTradeAble> buildTradeAbleList(Long futuresAccountId, List<String> tokenIds, List<FuturesPosition> positions, Header header) {
        Map<String, FuturesPosition> positionMap = buildPositionMap(positions);

        // 如果传入的tokenId列表为空，则返回所有tokenId的可交易信息
        List<String> resultTokenIds;
        if (CollectionUtils.isEmpty(tokenIds)) {
            resultTokenIds = positionMap.keySet().stream().map(key -> {
                // positionMap的key的规则是：tokenId_isLong
                // key = String.format("%s_%s", t.getTokenId(), t.getIsLong()
                String[] arrays = StringUtils.split(key, "_");
                return arrays[0];
            }).distinct().collect(Collectors.toList());
        } else {
            resultTokenIds = new ArrayList<>(tokenIds);
        }

        List<TokenFuturesInfo> infos = resultTokenIds.stream().map(tokenId -> {
            return basicService.getTokenFuturesInfoMap().get(tokenId);
        }).filter(Objects::nonNull).collect(Collectors.toList());

        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setAccountId(futuresAccountId)
                .addAllTokenId(infos.stream().map(TokenFuturesInfo::getCoinToken).collect(Collectors.toList()))
                .build();
        List<BalanceDetail> balanceDetails = grpcBalanceService.getBalanceDetail(request).getBalanceDetailsList();
        Map<String, BalanceDetail> balanceDetailMap;
        if(balanceDetails != null) {
            balanceDetailMap = balanceDetails.stream().collect(Collectors.toMap(balanceDetail -> balanceDetail.getTokenId(), balanceDetail -> balanceDetail));
        } else {
            balanceDetailMap = new HashMap<>();
        }
        return infos.stream().map(info -> {
            String tokenId = info.getTokenId();
            BalanceDetail balance = balanceDetailMap.get(info.getCoinToken());
            BigDecimal coinAvailable = (balance != null ? DecimalUtil.toBigDecimal(balance.getAvailable()) : BigDecimal.ZERO);
            String longKey = String.format("%s_%s", tokenId, FuturesUtil.LONG_VALUE);
            String shortKey = String.format("%s_%s", tokenId, FuturesUtil.SHORT_VALUE);
            FuturesPosition longPosition = positionMap.get(longKey);
            FuturesPosition shortPosition = positionMap.get(shortKey);
            return buildTradeAble(tokenId, coinAvailable, longPosition, shortPosition, header);
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private ProfitLossResult createProfitLossResult(FuturesTradeAble result, BigDecimal coinAvailable, SymbolDetail symbolDetail) {
        ProfitLossResult l = result.getLongProfitLoss();
        ProfitLossResult s = result.getShortProfitLoss();
        BigDecimal margin = FuturesUtil.getMargin(l).add(FuturesUtil.getMargin(s));
        BigDecimal orderMargin = FuturesUtil.getOrderMargin(l).add(FuturesUtil.getOrderMargin(s));
        BigDecimal realizedProfit = FuturesUtil.getRealisedPnl(l).add(FuturesUtil.getRealisedPnl(s));
        BigDecimal unrealizedProfit = FuturesUtil.getUnrealisedPnl(l).add(FuturesUtil.getUnrealisedPnl(s));

        DecimalStringFormatter marginFmt = new DecimalStringFormatter(symbolDetail.getMarginPrecision());

        return ProfitLossResult.newBuilder()
                .setCoinAvailable(marginFmt.format(coinAvailable))
                .setMargin(marginFmt.format(margin))
                .setOrderMargin(marginFmt.format(orderMargin))
                .setRealisedPnl(marginFmt.format(realizedProfit))
                .setUnrealisedPnl(marginFmt.format(unrealizedProfit))
                .build();
    }

    private ProfitLossResult getProfitLossResult(FuturesPosition position) {
        return ProfitLossResult.newBuilder()
                .setCoinAvailable(position.getCoinAvailable())
                .setMargin(position.getMargin())
                .setOrderMargin(position.getOrderMargin())
                .setRealisedPnl(position.getRealisedPnl())
                .setUnrealisedPnl(position.getUnrealisedPnl())
                .build();
    }


    public GetFuturesCoinAssetResponse getFuturesCoinAsset(GetFuturesCoinAssetRequest request) {
        if (!BrokerService.checkModule(request.getHeader(), FunctionModule.FUTURES)) {
            return GetFuturesCoinAssetResponse.getDefaultInstance();
        }
        Header header = request.getHeader();
        Long accountId = accountService.getFuturesIndexAccountId(header, request.getAccountIndex());
        List<String> tokenIds = request.getTokenIdsList();

        io.bhex.base.account.FuturesPositionsRequest req = io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setAccountId(accountId)
                // 这里请求的tokenIds应该是期货的symboIds，而不是币种。
                // 所以不能把tokenIds作为参数传给io.bhex.base.account.FuturesPositionsRequest
                // 因此在这里就默认取用户下面所有的持仓信息，再根据返回的持仓信息来计算币种对应的保证金
                //.addAllTokenIds(tokenIds)
                .setIsLong(-1)  // -1 为 all
                .build();
        io.bhex.base.account.FuturesPositionsResponse res = grpcFuturesOrderService.getFuturesPositions(req);

        Map<String, List<FuturesPosition>> positionMap = Optional.ofNullable(res.getPositionsList()).orElse(new ArrayList<>())
                .stream()
                .map(position -> toPosition(request.getHeader(), position))
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(position -> {
                    return position.getTokenId().toUpperCase();
                }));

        GetBalanceDetailRequest balanceRequest = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setAccountId(accountId)
                .addAllTokenId(tokenIds)
                .build();
        List<BalanceDetail> balanceDetails = grpcBalanceService.getBalanceDetail(balanceRequest)
                .getBalanceDetailsList();
        List<FuturesCoinAsset> coinAssets = balanceDetails
                .stream()
                .filter(balanceDetail -> StringUtils.isNotEmpty(basicService.getTokenName(balanceDetail.getOrgId(), balanceDetail.getTokenId())))
                .map(balanceDetail -> {
                    //兼容小写，统一用大写来分组
                    String tokenId = balanceDetail.getTokenId().toUpperCase();
                    List<FuturesPosition> positions = new ArrayList<>();
                    List<SymbolDetail> symbolDetails = BasicService.futureSymbolByQuoteTokenIdMap.get(tokenId);
                    if (!CollectionUtils.isEmpty(symbolDetails)) {
                        // 因为配置转发的原因，会出现很多相同SymbolId的SymbolDetail
                        // 这样按照原来逻辑计算资产的时候会出现重复计算的问题
                        // 因此这里SymbolDetail的列表这里需要根据symbolId去重
                        // TODO: 将来资产计算逻辑这里需要重构下
                        List<String> symbolIds = symbolDetails.stream()
                                .map(SymbolDetail::getSymbolId).distinct().collect(Collectors.toList());
                        for (String symbolId : symbolIds) {
                            List<FuturesPosition> positionList = positionMap.get(symbolId);
                            if (!CollectionUtils.isEmpty(positionList)) {
                                positions.addAll(positionList);
                            }
                        }
                    }
                    return toFuturesCoinAsset(tokenId, balanceDetail, positions);
                }).collect(Collectors.toList());
        return GetFuturesCoinAssetResponse.newBuilder()
                .addAllFuturesCoinAsset(coinAssets)
                .build();
    }

    private FuturesCoinAsset toFuturesCoinAsset(String tokenId, BalanceDetail balance, List<FuturesPosition> positions) {
        BigDecimal coinTotal = (balance != null ? DecimalUtil.toBigDecimal(balance.getTotal()) : BigDecimal.ZERO);
        BigDecimal coinAvailable = (balance != null ? DecimalUtil.toBigDecimal(balance.getAvailable()) : BigDecimal.ZERO);

        BigDecimal longMargin = BigDecimal.ZERO;
        BigDecimal longOrderMargin = BigDecimal.ZERO;
        BigDecimal shortMargin = BigDecimal.ZERO;
        BigDecimal shortOrderMargin = BigDecimal.ZERO;

        // TODO: 暂时使用默认价格精度，目前没有快速的办法获取币种的精度，如果需要根据币种的精度差异显示的话，这里将来需要调整
        DecimalStringFormatter priceFmt = new DecimalStringFormatter(BrokerServerConstants.BASE_OPTION_PRECISION);

        if (CollectionUtils.isNotEmpty(positions)) {
            for (FuturesPosition p : positions) {
                if (p.getIsLong().equalsIgnoreCase(FuturesUtil.LONG_VALUE)) {
                    longMargin = longMargin.add(p.getMargin() != null ? new BigDecimal(p.getMargin()) : BigDecimal.ZERO);
                    longOrderMargin = longOrderMargin.add(p.getOrderMargin() != null ? new BigDecimal(p.getOrderMargin()) : BigDecimal.ZERO);
                }
                if (p.getIsLong().equalsIgnoreCase(FuturesUtil.SHORT_VALUE)) {
                    shortMargin = shortMargin.add(p.getMargin() != null ? new BigDecimal(p.getMargin()) : BigDecimal.ZERO);
                    shortOrderMargin = shortOrderMargin.add(p.getOrderMargin() != null ? new BigDecimal(p.getOrderMargin()) : BigDecimal.ZERO);
                }
            }
        }
        BigDecimal margin = longMargin.add(shortMargin);
        BigDecimal orderMargin = longOrderMargin.add(shortOrderMargin);
        return FuturesCoinAsset.newBuilder()
                .setTokenId(tokenId)
                .setTokenName(tokenId)
                .setTotal(priceFmt.format(coinTotal))
                .setAvailableMargin(priceFmt.format(coinAvailable))
                .setPositionMargin(priceFmt.format(margin))
                .setOrderMargin(priceFmt.format(orderMargin))
                .build();
    }

    public AddMarginResponse addMargin(AddMarginRequest request) {
        Header header = request.getHeader();
        // 如果是反向合约，需要把请求的is_long取反，获取真正的值
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                request.getHeader().getOrgId(), request.getSymbolId());
        boolean realIsLong = symbolDetail.getIsReverse() != request.getIsLong();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && request.getAccountIndex() != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(request.getAccountType()), request.getAccountIndex())
//                : accountService.getFuturesAccountId(header);
        Long accountId = accountService.getFuturesIndexAccountId(header, request.getAccountIndex());

        BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(request.getAmount()) ? request.getAmount() : "");
        io.bhex.base.account.AddMarginRequest req = io.bhex.base.account.AddMarginRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setFuturesId(request.getSymbolId())
                .setIsLong(realIsLong)
                .setAmount(request.getAmount())
                .setExchangeId(symbolDetail.getExchangeId())
                .build();
        io.bhex.base.account.AddMarginResponse response = grpcFuturesOrderService.addMargin(req);
        return AddMarginResponse.newBuilder()
                .setSuccess(response.getSuccess())
                .build();
    }

    public ReduceMarginResponse reduceMargin(ReduceMarginRequest request) {
        Header header = request.getHeader();
        // 如果是反向合约，需要把请求的is_long取反，获取真正的值
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                request.getHeader().getOrgId(), request.getSymbolId());
        boolean realIsLong = symbolDetail.getIsReverse() != request.getIsLong();
//        Long accountId = header.getPlatform() == Platform.OPENAPI ?
//                accountService.getAccountId(header.getOrgId(), header.getOrgId(), AccountType.fromAccountTypeEnum(request.getAccountType()), request.getAccountIndex())
//                : accountService.getFuturesAccountId(header);
        Long accountId = accountService.getFuturesIndexAccountId(header, request.getAccountIndex());
        // 平台调整，减少保证金也调用"addMargin"接口，所以需要将amount取负数
        BigDecimal amount = new BigDecimal(request.getAmount());
        if (amount.compareTo(BigDecimal.ZERO) > 0) {
            amount = BigDecimal.ZERO.subtract(amount);
        }

        BigDecimalUtil.checkParamScale(amount != null ? amount.toPlainString() : "");
        io.bhex.base.account.AddMarginRequest req = io.bhex.base.account.AddMarginRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setAccountId(accountId)
                .setFuturesId(request.getSymbolId())
                .setIsLong(realIsLong)
                .setAmount(amount.toPlainString())
                .setExchangeId(symbolDetail.getExchangeId())
                .build();
        io.bhex.base.account.AddMarginResponse response = grpcFuturesOrderService.addMargin(req);
        return ReduceMarginResponse.newBuilder()
                .setSuccess(response.getSuccess())
                .build();
    }

    public GetPlanOrderResponse getPlanOrder(GetPlanOrderRequest request) {
        Header header = request.getHeader();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && request.getAccountIndex() != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(request.getAccountType()), request.getAccountIndex())
//                : accountService.getFuturesAccountId(header);
        Long accountId = accountService.getFuturesIndexAccountId(header, request.getAccountIndex());
        io.bhex.base.account.GetPlanOrderRequest req = io.bhex.base.account.GetPlanOrderRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .setOrderId(request.getOrderId())
                .setClientOrderId(request.getClientOrderId())
                .build();
        GetPlanOrderReply reply = grpcFuturesOrderService.getPlanOrder(req);
        if (!reply.hasPlanOrder()) {
            return GetPlanOrderResponse.newBuilder().build();
        }

        io.bhex.base.account.PlanOrder planOrder = reply.getPlanOrder();
        String key = String.format("%s_%s", request.getHeader().getOrgId(), planOrder.getSymbolId());
        Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
        SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, planOrder.getSymbolId());
        if (symbolDetail == null) {
            log.warn("getSymbolDetailFutures null. exchangeId:{}, symbolId:{}", exchangeId, planOrder.getSymbolId());
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        return GetPlanOrderResponse.newBuilder()
                .setPlanOrder(FuturesUtil.toPlanOrder(planOrder, symbolDetail))
                .build();
    }

    public GetInsuranceFundsResponse getInsuranceFunds(GetInsuranceFundsRequest request) {
        io.bhex.base.account.GetInsuranceFundsRequest req = io.bhex.base.account.GetInsuranceFundsRequest
                .newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setTokenId(request.getTokenId())
                .setFromId(request.getFromId())
                .setEndId(request.getEndId())
                .setLimit(request.getLimit())
                .build();
        io.bhex.base.account.GetInsuranceFundsResponse response = grpcFuturesOrderService.getInsuranceFunds(req);
        List<InsuranceFund> funds = response.getFundList()
                .stream()
                .map(FuturesUtil::toInsuranceFund)
                .collect(Collectors.toList());
        return GetInsuranceFundsResponse.newBuilder()
                .addAllFund(funds)
                .build();

    }

    public GetFundingRatesResponse getFundingRates(GetFundingRatesRequest request) {
        // TODO: 向平台请求资金费率需要加上orgId的过滤
        io.bhex.base.account.GetFundingRatesRequest req = io.bhex.base.account.GetFundingRatesRequest
                .newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .build();
        io.bhex.base.account.GetFundingRatesResponse resp = grpcFuturesOrderService.getFundingRates(req);
        List<FundingRate> rates = resp.getFundingRatesList().stream()
                .map(r -> toFundingRate(r, request.getHeader().getOrgId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return GetFundingRatesResponse.newBuilder().addAllFundingInfo(rates).build();
    }

    public GetHistoryFundingRatesResponse getHistoryFundingRates(GetHistoryFundingRatesRequest request) {
        io.bhex.base.account.GetHistoryFundingRatesRequest req = io.bhex.base.account.GetHistoryFundingRatesRequest
                .newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                .setTokenId(request.getTokenId())
                .setFromId(request.getFromId())
                .setEndId(request.getEndId())
                .setLimit(request.getLimit())
                .build();
        io.bhex.base.account.GetHistoryFundingRatesResponse resp = grpcFuturesOrderService.getHistoryFundingRates(req);
        List<HistoryFundingRate> rates = resp.getHistoryFundingRatesList().stream()
                .map(r -> toHistoryFundingRate(r, request.getHeader().getOrgId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return GetHistoryFundingRatesResponse.newBuilder().addAllFundingInfo(rates).build();
    }

    public GetHistoryInsuranceFundBalancesResponse getHistoryInsuranceFundBalances(GetHistoryInsuranceFundBalancesRequest request) {
        GetInsuranceFundConfigRequest fundConfigRequest = GetInsuranceFundConfigRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(request.getHeader()))
                //.setExchangeId(request.getExchangeId())
                .setSymbolId(request.getSymbolId())
                .build();
        GetInsuranceFundConfigResponse configResponse = grpcFuturesOrderService.getInsuranceFundConfig(fundConfigRequest);
        if (configResponse.getIncomeAccountId() > 0) {
            Date startDate = request.getFromId() > 0 ? new Date(request.getFromId()) : null;
            Date endDate = request.getEndId() > 0 ? new Date(request.getEndId()) : null;
            List<InsuranceFundBalanceSnap> list = statisticsBalanceService.queryFutureInsuranceFundBalanceSnapList(request.getHeader().getOrgId(), configResponse.getExchangeId(),
                    configResponse.getIncomeAccountId(), configResponse.getTokenId(), startDate, endDate, request.getLimit());
            if (CollectionUtils.isNotEmpty(list)) {
                List<GetHistoryInsuranceFundBalancesResponse.InsuranceFundBalance> balances = list.stream().map(b -> {
                    return GetHistoryInsuranceFundBalancesResponse.InsuranceFundBalance.newBuilder()
                            .setId(b.getDate().getTime())
                            .setTime(b.getDate().getTime())
                            .setTokenId(b.getTokenId())
                            .setAvailable(b.getAvailable().stripTrailingZeros().toPlainString())
                            .build();
                }).sorted(Comparator.comparing(GetHistoryInsuranceFundBalancesResponse.InsuranceFundBalance::getId).reversed()).collect(Collectors.toList());
                return GetHistoryInsuranceFundBalancesResponse.newBuilder().addAllInsuranceFundBalance(balances).build();
            }
        }

        return GetHistoryInsuranceFundBalancesResponse.getDefaultInstance();
    }


    public GetFuturesI18nNameResponse getFuturesI18nName(GetFuturesI18nNameRequest request) {
        Internationalization i18n = internationalizationMapper.queryInternationalizationByKey(
                request.getInKey(), request.getEnv());

        GetFuturesI18nNameResponse.Builder responseBuilder = GetFuturesI18nNameResponse.newBuilder()
                .setInKey(request.getInKey())
                .setEnv(request.getEnv());
        if (i18n != null) {
            return responseBuilder.setInValue(i18n.getInValue()).build();
        } else {
            if ("en".equals(request.getEnv())) {
                return responseBuilder.setInValue(request.getInKey()).build();
            } else {
                // 如果非英文环境未能查询到国际化配置，先查询英文配置，没有再返回InKey
                Internationalization i18nEN = internationalizationMapper.queryInternationalizationByKey(
                        request.getInKey(), "en");
                if (i18nEN != null) {
                    return responseBuilder.setInValue(i18nEN.getInValue()).build();
                } else {
                    return responseBuilder.setInValue(request.getInKey()).build();
                }
            }
        }
    }

    public GetFuturesBestOrderResponse getFuturesBestOrder(Header header, Long exchangeId, String symbolId) {
        Long accountId = accountService.getFuturesAccountId(header);

        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(header.getOrgId(), symbolId);
        if (symbolDetail == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        GetBestOrderRequest request = GetBestOrderRequest.newBuilder()
                .setAccountId(accountId)
                .setExchangeId(exchangeId)
                .setSymbolId(symbolId)
                .build();
        GetBestOrderResponse response = grpcOrderService.getBestOrder(request);
        return GetFuturesBestOrderResponse.newBuilder()
                .setPrice(response.getPrice())
                .setAsk(getBrokerOrderFromBestOrder(response.getAsk(), symbolDetail))
                .setBid(getBrokerOrderFromBestOrder(response.getBid(), symbolDetail))
                .build();
    }

    private Order getBrokerOrderFromBestOrder(io.bhex.base.account.Order bestOrder, SymbolDetail symbolDetail) {
        DecimalStringFormatter minPriceFmt = new DecimalStringFormatter(
                DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()));
        DecimalStringFormatter baseFmt = new DecimalStringFormatter(
                DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()));

        String priceVal = symbolDetail.getIsReverse() ?
                minPriceFmt.reciprocalFormat(bestOrder.getPrice())
                : minPriceFmt.format(bestOrder.getPrice());

        return Order.newBuilder()
                .setTime(bestOrder.getCreatedTime())
                .setAccountId(bestOrder.getAccountId())
                .setSymbolId(bestOrder.getSymbolId())
                .setOrderId(bestOrder.getOrderId())
                .setOrigQty(baseFmt.format(bestOrder.getQuantity()))
                .setOrderSide(OrderSide.valueOf(bestOrder.getSide().name()))
                .setPrice(priceVal)
                .build();
    }

    private FundingRate toFundingRate(io.bhex.base.account.FundingRate bhFundingRate, Long orgId) {
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                orgId, bhFundingRate.getTokenId());
        if (symbolDetail == null) {
            return null;
        }

        BigDecimal settleRate = DecimalUtil.toBigDecimal(bhFundingRate.getSettleRate());
        BigDecimal rate = DecimalUtil.toBigDecimal(bhFundingRate.getFundingRate());
        if (symbolDetail.getIsReverse()) {
            settleRate = settleRate.negate();
            rate = rate.negate();
        }

        return FundingRate.newBuilder()
                .setTokenId(bhFundingRate.getTokenId())
                .setLastSettleTime(bhFundingRate.getLastSettleTime())
                .setSettleRate(DecimalUtil.toTrimString(settleRate))
                .setNextSettleTime(bhFundingRate.getNextSettleTime())
                .setFundingRate(DecimalUtil.toTrimString(rate))
                .build();
    }

    private HistoryFundingRate toHistoryFundingRate(io.bhex.base.account.HistoryFundingRate bhFundingRate, Long orgId) {
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                orgId, bhFundingRate.getTokenId());
        if (symbolDetail == null) {
            return null;
        }

        BigDecimal settleRate = DecimalUtil.toBigDecimal(bhFundingRate.getSettleRate());
        if (symbolDetail.getIsReverse()) {
            settleRate = settleRate.negate();
        }

        return HistoryFundingRate.newBuilder()
                .setId(bhFundingRate.getId())
                .setTokenId(bhFundingRate.getTokenId())
                .setSettleRate(DecimalUtil.toTrimString(settleRate))
                .setSettleTime(bhFundingRate.getSettleTime())
                .build();
    }

    public CalculateProfitInfoResponse calculateProfitInfo(CalculateProfitInfoRequest request) {
        SymbolDetail symbol = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                request.getHeader().getOrgId(), request.getSymbolId());
        if (symbol == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        DecimalStringFormatter marginFmt = new DecimalStringFormatter(symbol.getMarginPrecision());

        CalculateProfitInfoResponse.Builder responseBuilder = CalculateProfitInfoResponse.newBuilder();

        BigDecimal multiplier = DecimalUtil.toBigDecimal(symbol.getFuturesMultiplier());
        BigDecimal openQuantity = new BigDecimal(request.getOpenQuantity());
        BigDecimal closeQuantity = new BigDecimal(request.getCloseQuantity());
        BigDecimal openPrice = new BigDecimal(request.getOpenPrice());
        BigDecimal closePrice = new BigDecimal(request.getClosePrice());
        BigDecimal leverage = new BigDecimal(request.getLeverage());

        if (openQuantity.compareTo(BigDecimal.ZERO) <= 0
                || closeQuantity.compareTo(BigDecimal.ZERO) <= 0
                || openPrice.compareTo(BigDecimal.ZERO) <= 0
                || closePrice.compareTo(BigDecimal.ZERO) <= 0
                || leverage.compareTo(BigDecimal.ZERO) <= 0
                || closeQuantity.compareTo(openQuantity) > 0) {
            log.warn("calculateProfitInfo - invalid request: {}", TextFormat.shortDebugString(request));
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        // 计算开仓保证金
        BigDecimal orderMargin;
        if (symbol.getIsReverse()) {
            // 反向合约：合约乘数 * 开仓数量 /（杠杆 * 开仓价格）（合约币种）
            orderMargin = multiplier.multiply(openQuantity).divide(leverage.multiply(openPrice), 18, RoundingMode.DOWN);
        } else {
            // 正向合约：合约乘数 * 开仓数量 * 开仓价格 / 杠杆 （USDT)
            orderMargin = multiplier.multiply(openQuantity).multiply(openPrice).divide(leverage, 18, RoundingMode.DOWN);
        }
        responseBuilder.setOrderMargin(marginFmt.format(orderMargin));


        SymbolFeeConfig symbolFeeConfig = brokerFeeService.querySymbolFeeConfig(request.getHeader().getOrgId(), symbol.getExchangeId(),
                symbol.getSymbolId());
        if (symbolFeeConfig == null) {
            log.warn("calculateProfitRate - can not find SymbolFeeConfig. orgId: {} symbolId: {} exchangeId: {}",
                    request.getHeader().getOrgId(), symbol.getSymbolId(), symbol.getExchangeId());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        // 计算开仓的Taker，Maker手续费
        BigDecimal takerOpenFeeRate, makerOpenFeeRate;
        if (request.getIsLong()) {
            takerOpenFeeRate = symbolFeeConfig.getTakerBuyFee();
            makerOpenFeeRate = symbolFeeConfig.getMakerBuyFee();
        } else {
            takerOpenFeeRate = symbolFeeConfig.getTakerSellFee();
            makerOpenFeeRate = symbolFeeConfig.getMakerSellFee();
        }

        BigDecimal takerOpenFee = calculateTakerMakerFee(multiplier, openQuantity,
                openPrice, takerOpenFeeRate, symbol.getIsReverse());
        BigDecimal makerOpenFee = calculateTakerMakerFee(multiplier, openQuantity,
                openPrice, makerOpenFeeRate, symbol.getIsReverse());
        responseBuilder.setTakerOpenFee(marginFmt.format(takerOpenFee));
        responseBuilder.setMakerOpenFee(marginFmt.format(makerOpenFee));

        // 计算平仓的Taker，Maker手续费
        BigDecimal takerCloseFeeRate, makerCloseFeeRate;
        if (request.getIsLong()) {
            takerCloseFeeRate = symbolFeeConfig.getTakerSellFee();
            makerCloseFeeRate = symbolFeeConfig.getMakerSellFee();
        } else {
            takerCloseFeeRate = symbolFeeConfig.getTakerBuyFee();
            makerCloseFeeRate = symbolFeeConfig.getMakerBuyFee();
        }

        BigDecimal takerCloseFee = calculateTakerMakerFee(multiplier, closeQuantity,
                closePrice, takerCloseFeeRate, symbol.getIsReverse());
        BigDecimal makerCloseFee = calculateTakerMakerFee(multiplier, closeQuantity,
                closePrice, makerCloseFeeRate, symbol.getIsReverse());
        responseBuilder.setTakerCloseFee(marginFmt.format(takerCloseFee));
        responseBuilder.setMakerCloseFee(marginFmt.format(makerCloseFee));

        // 计算收益
        BigDecimal profit;
        if (symbol.getIsReverse()) {
            BigDecimal reciprocalOpenPrice = openPrice.compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : BigDecimal.ONE.divide(openPrice, 18, RoundingMode.DOWN);
            BigDecimal reciprocalClosePrice = closePrice.compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : BigDecimal.ONE.divide(closePrice, 18, RoundingMode.DOWN);

            if (request.getIsLong()) {
                // 反向合约-多仓：（1/开仓价格-1/平仓价格）*合约乘数*平仓数量（合约币种）
                profit = reciprocalOpenPrice.subtract(reciprocalClosePrice).multiply(multiplier).multiply(closeQuantity);
            } else {
                // 反向合约-空仓： （1/平仓价格 - 1/开仓价格）*合约乘数*平仓数量（合约币种）
                profit = reciprocalClosePrice.subtract(reciprocalOpenPrice).multiply(multiplier).multiply(closeQuantity);
            }
        } else {
            if (request.getIsLong()) {
                // 正向合约-多仓：（平仓价格 - 开仓价格）* 合约乘数 * 平仓数量 （USDT）
                profit = closePrice.subtract(openPrice).multiply(multiplier).multiply(closeQuantity);
            } else {
                // 正向合约-空仓： （开仓价格 - 平仓价格）* 合约乘数 * 平仓数量（USDT）
                profit = openPrice.subtract(closePrice).multiply(multiplier).multiply(closeQuantity);
            }
        }
        responseBuilder.setProfit(marginFmt.format(profit));

        // 计算收益率
        BigDecimal profitRate = profit.divide(orderMargin, 18, RoundingMode.DOWN);
        responseBuilder.setProfitRate(profitRate.setScale(4, RoundingMode.DOWN).stripTrailingZeros().toPlainString());

        // 设置quoteToken
        responseBuilder.setTokenId(symbol.getQuoteTokenId());

        return responseBuilder.build();
    }

    private BigDecimal calculateTakerMakerFee(BigDecimal multiplier, BigDecimal quantity, BigDecimal price,
                                              BigDecimal feeRate, boolean isReverse) {
        if (isReverse) {
            // 反向合约：（ 合约乘数 * 开平仓数量 /（开平仓价格））* Taker/Maker 费率（合约币种）
            return multiplier.multiply(quantity).divide(price, RoundingMode.DOWN).multiply(feeRate);
        } else {
            // 正向合约：合约乘数 * 开平仓数量 * 开平仓价格 * Taker/Maker 费率 （USDT）
            return multiplier.multiply(quantity).multiply(price).multiply(feeRate);
        }
    }

    public CalculateLiquidationPriceResponse calculateLiquidationPrice(CalculateLiquidationPriceRequest request) {
        SymbolDetail symbol = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(
                request.getHeader().getOrgId(), request.getSymbolId());
        if (symbol == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        TokenFuturesInfo tokenFuturesInfo = basicService.getTokenFuturesInfoMap().get(symbol.getSymbolId());
        if (tokenFuturesInfo == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        SymbolFeeConfig symbolFeeConfig = brokerFeeService.querySymbolFeeConfig(request.getHeader().getOrgId(), symbol.getExchangeId(), symbol.getSymbolId());
        if (symbolFeeConfig == null) {
            log.warn("calculateProfitRate - can not find SymbolFeeConfig. orgId: {} symbolId: {} exchangeId: {}",
                    request.getHeader().getOrgId(), symbol.getSymbolId(), symbol.getExchangeId());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        DecimalStringFormatter priceFmt = new DecimalStringFormatter(symbol.getMinPricePrecision());

        BigDecimal multiplier = DecimalUtil.toBigDecimal(symbol.getFuturesMultiplier());
        BigDecimal openQuantity = new BigDecimal(request.getOpenQuantity());
        BigDecimal openPrice = new BigDecimal(request.getOpenPrice());
        BigDecimal leverage = new BigDecimal(request.getLeverage());
        BigDecimal marginCall = new BigDecimal(request.getMarginCall());

        if (openQuantity.compareTo(BigDecimal.ZERO) <= 0
                || openPrice.compareTo(BigDecimal.ZERO) <= 0
                || marginCall.compareTo(BigDecimal.ZERO) < 0
                || leverage.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("calculateProfitInfo - invalid request: {}", TextFormat.shortDebugString(request));
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        // 根据风险限额ID查询当前的风险限额档位对应的维持保证金率
        io.bhex.base.proto.FuturesRiskLimit futuresRiskLimit = tokenFuturesInfo.getRiskLimitsList()
                .stream()
                .filter(r -> r.getRiskLimitId() == request.getRiskLimitId())
                .findFirst().orElse(null);
        if (futuresRiskLimit == null) {
            log.warn("calculateLiquidationPrice -  can not find FuturesRiskLimit by id: {}", request.getRiskLimitId());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        BigDecimal matainMarginRate = DecimalUtil.toBigDecimal(futuresRiskLimit.getMaintainMargin());

        // 计算手续费
        BigDecimal feeRate = request.getIsLong() ? symbolFeeConfig.getTakerBuyFee() : symbolFeeConfig.getTakerSellFee();
        BigDecimal fee = calculateTakerMakerFee(multiplier, openQuantity, openPrice, feeRate, symbol.getIsReverse());

        BigDecimal liquidationPrice;
        if (symbol.getIsReverse()) {
            /*
             * 反向合约
             *
             * 起始保证金：合约乘数*开仓数量/（杠杆 * 开仓价格）（合约币种）
             * 开仓价值 = 合约乘数 * 开仓数量/ 开仓价格（合约币种）
             * 维持保证金率 = 对应档位的维持保证金率
             *
             * 多仓： ((1 + 维持保证金率) * 合约乘数 * 手数) / (开仓价值 + 起始保证金 - 手续费 + 追加保证金)
             * 空仓： ((1 - 维持保证金率) * 合约乘数 * 手数) / (开仓价值 - 起始保证金 + 手续费 - 追加保证金)
             */
            BigDecimal initMargin = multiplier.multiply(openQuantity)
                    .divide(leverage.multiply(openPrice), 18, RoundingMode.DOWN);
            BigDecimal openValue = multiplier.multiply(openQuantity).divide(openPrice, 18, RoundingMode.DOWN);

            if (request.getIsLong()) {
                liquidationPrice = BigDecimal.ONE.add(matainMarginRate).multiply(multiplier).multiply(openQuantity)
                        .divide(openValue.add(initMargin).subtract(fee).add(marginCall), 18, RoundingMode.DOWN);
            } else {
                liquidationPrice = BigDecimal.ONE.subtract(matainMarginRate).multiply(multiplier).multiply(openQuantity)
                        .divide(openValue.subtract(initMargin).add(fee).subtract(marginCall), 18, RoundingMode.DOWN);
            }
        } else {
            /*
             * 正向合约
             *
             * 起始保证金 = 合约乘数*开仓数量*开仓价格/杠杆 （USDT)
             * 开仓价值 = 开仓价格 * 合约乘数* 开仓数量（USDT)
             * 维持保证金率 = 对应档位的维持保证金率
             *
             * 多仓： (开仓价值 - 起始保证金 + 手续费 - 追加保证金) / ((1 - 维持保证金率) * 合约乘数 * 手数)
             * 空仓： (开仓价值 + 起始保证金 - 手续费 + 追加保证金) / ((1 + 维持保证金率) * 合约乘数 * 手数)
             */
            BigDecimal initMargin = multiplier.multiply(openQuantity).multiply(openPrice).divide(leverage, 18, RoundingMode.DOWN);
            BigDecimal openValue = openPrice.multiply(multiplier).multiply(openQuantity);

            if (request.getIsLong()) {
                liquidationPrice = openValue
                        .subtract(initMargin)
                        .add(fee)
                        .subtract(marginCall)
                        .divide(BigDecimal.ONE
                                        .subtract(matainMarginRate)
                                        .multiply(multiplier)
                                        .multiply(openQuantity),
                                18, RoundingMode.DOWN);
            } else {
                liquidationPrice = openValue
                        .add(initMargin)
                        .subtract(fee)
                        .add(marginCall)
                        .divide(BigDecimal.ONE
                                        .add(matainMarginRate)
                                        .multiply(multiplier)
                                        .multiply(openQuantity),
                                18, RoundingMode.DOWN);
            }
        }
        return CalculateLiquidationPriceResponse.newBuilder()
                .setLiquidationPrice(priceFmt.format(liquidationPrice)).build();
    }

    public MarketPullFuturesPositionsResponse marketPullFuturesPositions(Header header, List<String> symbolIds, Long fromPositionId, Long limit) {
        // 暂时支持一个币对
        String symbolId = CollectionUtils.isEmpty(symbolIds) ? null : symbolIds.get(0);
        if (symbolId == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        String key = String.format("%s_%s", header.getOrgId(), symbolId);
        Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
        if (exchangeId == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        PullFuturesPositionRequest request = PullFuturesPositionRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setExchangeId(exchangeId)
                .addSymbolIds(symbolId)
                .setFromPositionId(fromPositionId)
                .setSize(limit)
                .setSortByTotal(true)
                .build();
        PullFuturesPositionoReply response = grpcFuturesOrderService.pullFuturesPosition(request);
        List<FuturesPosition> positions = response.getFuturesPositionListList()
                .stream()
                .map(p -> toPosition(header, p))
                .collect(Collectors.toList());
        return MarketPullFuturesPositionsResponse.newBuilder()
                .addAllPositions(positions)
                .build();
    }

    public SetStopProfitLossResponse setStopProfitLoss(SetStopProfitLossRequest request) {
        BigDecimal stopProfitPrice;
        BigDecimal stopLossPrice;
        try {
            stopProfitPrice = StringUtils.isNotEmpty(request.getStopProfitPrice()) ?
                    new BigDecimal(request.getStopProfitPrice()) : null;
            stopLossPrice = StringUtils.isNotEmpty(request.getStopLossPrice()) ?
                    new BigDecimal(request.getStopLossPrice()) : null;
        } catch (NumberFormatException e) {
            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_ILLEGAL);
        }

        SymbolDetail symbol = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(request.getHeader().getOrgId(),
                request.getSymbolId());
        if (symbol == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        PlanOrder.PlanOrderTypeEnum stopProfitPlanOrderType = request.getIsLong() ?
                PlanOrder.PlanOrderTypeEnum.STOP_LONG_PROFIT : PlanOrder.PlanOrderTypeEnum.STOP_SHORT_PROFIT;
        PlanOrder.PlanOrderTypeEnum stopLossPlanOrderType = request.getIsLong() ?
                PlanOrder.PlanOrderTypeEnum.STOP_LONG_LOSS : PlanOrder.PlanOrderTypeEnum.STOP_SHORT_LOSS;

        // 校验止盈止损价格是否合理
        BigDecimal stopProfitQuotePrice = null;
        if (stopProfitPrice != null) {
            stopProfitQuotePrice = getPlanOrderQuotePrice(symbol, request.getStopProfitTriggerCondition(), request.getHeader().getOrgId());
            checkStopProfitLossTriggerPrice(stopProfitPrice, stopProfitPlanOrderType, stopProfitQuotePrice);
        }

        BigDecimal stopLossQuotePrice = null;
        if (stopLossPrice != null) {
            stopLossQuotePrice = getPlanOrderQuotePrice(symbol, request.getStopLossTriggerCondition(), request.getHeader().getOrgId());
            checkStopProfitLossTriggerPrice(stopLossPrice, stopLossPlanOrderType, stopLossQuotePrice);
        }

        StopProfitLossSetting setting = getStopProfitLossSetting(request.getHeader(),
                request.getSymbolId(), request.getIsLong());

        StopProfitLossSetting.Builder newSettingBuilder = StopProfitLossSetting.newBuilder();
        newSettingBuilder.setIsLong(request.getIsLong());
        newSettingBuilder.setSymbolId(request.getSymbolId());

        // 取消之前设置的止盈订单
        if (setting.getIsStopProfit()) {
            cancelFuturesOrder(request.getHeader(), 0,
                    setting.getStopProfitPlanOrderId(), "", PlanOrder.FuturesOrderType.STOP);
        }

        // 设置新的止盈订单
        if (stopProfitPrice != null) {
            PlanOrder planOrder = createStopProfitLossOrder(request, stopProfitQuotePrice, true,
                    stopProfitPlanOrderType, symbol);
            newSettingBuilder.setIsStopProfit(true);
            newSettingBuilder.setStopProfitPrice(planOrder.getTriggerPrice());
            newSettingBuilder.setStopProfitPlanOrderId(planOrder.getOrderId());
            newSettingBuilder.setStopProfitCloseType(planOrder.getCloseType());
            newSettingBuilder.setStopProfitTriggerCondition(planOrder.getTriggerCondition());
        }

        // 取消之前的止损订单
        if (setting.getIsStopLoss()) {
            cancelFuturesOrder(request.getHeader(), 0,
                    setting.getStopLossPlanOrderId(), "", PlanOrder.FuturesOrderType.STOP);
        }

        // 设置新的止损订单
        if (stopLossPrice != null) {
            PlanOrder planOrder = createStopProfitLossOrder(request, stopLossQuotePrice, false,
                    stopLossPlanOrderType, symbol);
            newSettingBuilder.setIsStopLoss(true);
            newSettingBuilder.setStopLossPrice(planOrder.getTriggerPrice());
            newSettingBuilder.setStopLossPlanOrderId(planOrder.getOrderId());
            newSettingBuilder.setStopLossCloseType(planOrder.getCloseType());
            newSettingBuilder.setStopLossTriggerCondition(planOrder.getTriggerCondition());
        }

        return SetStopProfitLossResponse.newBuilder()
                .setSetting(newSettingBuilder.build())
                .build();
    }

    public StopProfitLossSetting getStopProfitLossSetting(Header header, String symbolId, boolean isLong) {
        Long futuresAccountId = accountService.getFuturesAccountId(header);
        List<PlanOrder.PlanOrderTypeEnum> planOrderTypes;
        if (isLong) {
            planOrderTypes = Lists.newArrayList(
                    PlanOrder.PlanOrderTypeEnum.STOP_LONG_PROFIT,
                    PlanOrder.PlanOrderTypeEnum.STOP_LONG_LOSS
            );
        } else {
            planOrderTypes = Lists.newArrayList(
                    PlanOrder.PlanOrderTypeEnum.STOP_SHORT_PROFIT,
                    PlanOrder.PlanOrderTypeEnum.STOP_SHORT_LOSS
            );
        }
        List<PlanOrder> planOrders = queryFuturesPlanOrders(header, futuresAccountId, symbolId, 0L, 0L, 0L,
                0L, 10, OrderQueryType.CURRENT, planOrderTypes);

        StopProfitLossSetting.Builder settingBuilder = StopProfitLossSetting.newBuilder();
        planOrders.forEach(planOrder -> {
            switch (planOrder.getOrderType()) {
                case STOP_LONG_PROFIT:
                case STOP_SHORT_PROFIT:
                    settingBuilder.setIsStopProfit(true);
                    settingBuilder.setStopProfitPrice(planOrder.getTriggerPrice());
                    settingBuilder.setStopProfitPlanOrderId(planOrder.getOrderId());
                    settingBuilder.setStopProfitTriggerCondition(planOrder.getTriggerCondition());
                    settingBuilder.setStopProfitCloseType(planOrder.getCloseType());
                    break;
                case STOP_LONG_LOSS:
                case STOP_SHORT_LOSS:
                    settingBuilder.setIsStopLoss(true);
                    settingBuilder.setStopLossPrice(planOrder.getTriggerPrice());
                    settingBuilder.setStopLossPlanOrderId(planOrder.getOrderId());
                    settingBuilder.setStopLossTriggerCondition(planOrder.getTriggerCondition());
                    settingBuilder.setStopLossCloseType(planOrder.getCloseType());
                    break;
            }
        });

        return settingBuilder.build();
    }

    /**
     * 创建止盈止损计划委托订单
     */
    private PlanOrder createStopProfitLossOrder(SetStopProfitLossRequest request, BigDecimal quotePrice,
                                                boolean isStopProfit, PlanOrder.PlanOrderTypeEnum planOrderType,
                                                SymbolDetail symbolDetail) {
        Header header = request.getHeader();
        FuturesOrderSide orderSide = request.getIsLong() ? FuturesOrderSide.SELL_CLOSE : FuturesOrderSide.BUY_CLOSE;
        Long exchangeId = basicService.getOrgExchangeFuturesMap().get(
                String.format("%s_%s", header.getOrgId(), request.getSymbolId()));
        if (exchangeId == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        //下单方向
        OrderSideEnum side = FuturesUtil.getBHexOrderSide(orderSide);
        if (side == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        Long futuresAccountId = accountService.getFuturesAccountId(header);
        String clientOrderId = String.format("%s_%s", planOrderType, System.currentTimeMillis());

        String triggerPrice = isStopProfit ? request.getStopProfitPrice() : request.getStopLossPrice();
        PlanOrder.TriggerConditionEnum triggerCondition = isStopProfit ? request.getStopProfitTriggerCondition() : request.getStopLossTriggerCondition();
        PlanOrder.CloseTypeEnum closeType = isStopProfit ?
                request.getStopProfitCloseType() : request.getStopLossCloseType();
        CreateFuturesOrderRequest orderRequest = CreateFuturesOrderRequest.newBuilder()
                .setHeader(header)
                .setSymbolId(request.getSymbolId())
                .setClientOrderId(clientOrderId)
                .setFuturesOrderType(PlanOrder.FuturesOrderType.STOP)
                .setFuturesPriceType(FuturesPriceType.MARKET_PRICE)
                .setIsClose(true)
                .setExchangeId(exchangeId)
                .setQuantity("0")
                .setPrice("0")
                .setTriggerPrice(triggerPrice)
                .setPlanOrderType(planOrderType)
                .setTriggerCondition(triggerCondition)
                .setFuturesOrderSide(orderSide)
                .setCloseType(closeType)
                .build();

        boolean isTransferOrder = false;
        if (symbolDetail.getExchangeId() != symbolDetail.getMatchExchangeId()) {
            isTransferOrder = Boolean.TRUE;
        }
        MakerBonusConfig config = basicService.getMakerBonusConfig(header.getOrgId(), request.getSymbolId());
        BigDecimal makerBonusRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMakerBonusRate());
        BigDecimal minInterestFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinInterestFeeRate());
        BigDecimal minTakerFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinTakerFeeRate());

        TradeFeeConfig tradeFeeConfig = brokerFeeService.getOrderTradeFeeConfig(header.getOrgId(), exchangeId, request.getSymbolId(), isTransferOrder, header.getUserId(), futuresAccountId, AccountType.FUTURES,
                OrderSide.valueOf(side.name()), DEFAULT_TRADE_DEE_CONFIG, DEFAULT_TRANSFER_TRADE_DEE_CONFIG, makerBonusRate, minInterestFeeRate, minTakerFeeRate);

        NewPlanOrderRequest newPlanOrderRequest = FuturesUtil.createNewPlanOrderRequest(
                futuresAccountId, orderRequest, symbolDetail, quotePrice, tradeFeeConfig.getMakerFeeRate(), tradeFeeConfig.getTakerFeeRate(),
                tradeFeeConfig.getMakerFeeRate().compareTo(BigDecimal.ZERO) < 0 ? DecimalUtil.fromBigDecimal(makerBonusRate) : DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
        NewPlanOrderReply newPlanOrderReply = grpcFuturesOrderService.createFuturesPlanOrder(newPlanOrderRequest);
        return FuturesUtil.toPlanOrder(newPlanOrderReply.getOrder(), symbolDetail);
    }

    /**
     * 一键平仓
     *
     * @param header
     * @param symbolId
     * @param clientOrderId
     * @param isLong
     * @param orderSource
     * @return
     */
    public ClosePromptlyFutureOrderResponse closePromptlyFutureOrder(Header header, String symbolId, long accountId, int accountIndex,
                                                                     String clientOrderId, int isLong, String orderSource) {
        ClosePromptlyFutureOrderResponse response;
        //1. 查询持仓
        accountId = accountService.getFuturesIndexAccountId(header, accountIndex);
        final SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(header.getOrgId(), symbolId);
        if (symbolDetail == null) {
            log.warn("getSymbolDetailFuturesByOrgIdAndSymbolId null. orgId:{}, symbolId:{}", header.getOrgId(), symbolId);
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        Long exchangeId = symbolDetail.getExchangeId();

        io.bhex.base.account.FuturesPositionsRequest request = io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setAccountId(accountId)
                .addTokenIds(symbolId)
                .setIsLong(symbolDetail.getIsReverse() ? (isLong == 1 ? 0 : 1) : isLong)
                .setLimit(1)
                .build();
        //找其中一条持仓
        FuturesPosition position = checkAndGetPosition(request, header, accountId, symbolId, isLong);

        //2. 查询委托，如果有当前仓位的平仓委托，先撤单，然后一起全部市价全平仓
        OrderSideEnum querySide = OrderSideEnum.BUY;
        if (isLong == 1 && !symbolDetail.getIsReverse()) {
            querySide = OrderSideEnum.SELL;
        } else if (isLong == 0 && symbolDetail.getIsReverse()) {
            querySide = OrderSideEnum.SELL;
        }
        GetOrdersRequest getOrdersRequest = GetOrdersRequest.newBuilder()
                .setAccountId(accountId)
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setSymbolId(symbolId)
                .addOrderStatusList(OrderStatusEnum.NEW)
                .addOrderStatusList(OrderStatusEnum.PARTIALLY_FILLED)
                .addSide(querySide)
                .build();
        GetOrdersReply getOrdersReply = grpcFuturesOrderService.queryFuturesOrders(getOrdersRequest);
        final Long finalAccountId = accountId;
        final List<Order> canceleds = new LinkedList<>();
        //3. 撤单
        getOrdersReply.getOrdersList().stream().forEach(order -> {
            try {
                //开仓单不能撤销
                if (!order.getIsClose()) {
                    return;
                }
                //平仓
                CancelOrderReply reply = grpcFuturesOrderService.cancelFuturesOrder(CancelOrderRequest.newBuilder()
                        .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                        .setAccountId(finalAccountId)
                        .setClientOrderId(order.getClientOrderId())
                        .setOrderId(order.getOrderId())
                        .setOrderType(order.getType())
                        .build());
                //撤单成功
                if (reply.getStatus() == OrderStatusEnum.CANCELED) {
                    canceleds.add(getFuturesOrder(order, symbolDetail, exchangeId));
                }
                log.info("close promptly futures cancel order: {}", TextFormat.shortDebugString(order));
            } catch (Exception e) {
                log.info("error cancel order: " + TextFormat.shortDebugString(order), e);
            }
        });
        //4. 重新查询持仓，下市价平仓单
        position = checkAndGetPosition(request, header, accountId, symbolId, isLong);

        //5. 汇总撤单结果和市价平仓订单结果回来
        FuturesOrderSide side;
        if (isLong == 1) {
            side = FuturesOrderSide.SELL_CLOSE;
        } else {
            side = FuturesOrderSide.BUY_CLOSE;
        }
        CreateFuturesOrderRequest orderRequest = CreateFuturesOrderRequest.newBuilder()
                .setHeader(header)
                .setSymbolId(symbolId)
                .setAccountIndex(accountIndex)
                .setQuantity(position.getAvailable())
                .setIsClose(true)
                .setFuturesOrderSide(side)
                .setClientOrderId(clientOrderId)
                .setFuturesOrderType(PlanOrder.FuturesOrderType.LIMIT)//不判断，过入参校验
                .setFuturesPriceType(FuturesPriceType.MARKET_PRICE)
                .setExtraFlag(FuturesOrderExtraFlag.UNKNOWN_FLAG)//没用
                .setOrderSource(orderSource)
                .setPrice("0")
                .setTriggerPrice("0")
                .build();
        CreateFuturesOrderResponse orderResponse = newFuturesOrder(orderRequest);

        response = ClosePromptlyFutureOrderResponse.newBuilder()
                .addAllCancelOrder(canceleds)
                .setOrder(orderResponse.getOrder())
                .build();
        return response;
    }

    private FuturesPosition checkAndGetPosition(io.bhex.base.account.FuturesPositionsRequest request, Header header, Long accountId, String symbolId, Integer isLong) {
        List<FuturesPosition> positions = this.getBhexFuturesPositionsEx(header, request, accountId);
        //找其中一条持仓
        FuturesPosition position = null;
        if (positions != null && !positions.isEmpty()) {
            position = positions.get(0);
        }
        if (position == null) {
            log.warn("close promptly futures position is null, orgId:{}, symbolId:{}, isLong:{}", header.getOrgId(), symbolId, isLong);
            throw new BrokerException(BrokerErrorCode.ORDER_SPL_NO_POSITION);
        }
        log.info("close promptly futures position: {}", TextFormat.shortDebugString(position));
        if (new BigDecimal(position.getTotal()).compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("close promptly futures position is zero, orgId:{}, symbolId:{}, isLong:{}", header.getOrgId(), symbolId, isLong);
            throw new BrokerException(BrokerErrorCode.ORDER_SPL_POSITION_NOT_ENOUGH);
        }
        return position;
    }

    public void clearCancelByUser(Header header) {
        //获取所有合约子账户
        List<Account> accountList = accountService.querySubAccountList(header, AccountType.FUTURES);
        if (accountList.isEmpty()) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        CompletableFuture.runAsync(() -> {
            try {
                int orderLimit = 500;
                for (Account account : accountList) {
                    int breakCount = 0;
                    while (breakCount <= 5) {
                        try {
                            Set<String> symbols = new HashSet<>();
                            //查询当前订单
                            List<Order> orders = queryFuturesOrders(header, account.getAccountId(), "", 0L, 0L, 0L, 0L, "", "",
                                    null, orderLimit, OrderQueryType.CURRENT);
                            if (orders.isEmpty()) {
                                break;
                            }
                            symbols.addAll(orders.stream().map(Order::getSymbolId).collect(Collectors.toList()));
                            List<OrderSideEnum> sides = Lists.newArrayList(OrderSideEnum.BUY, OrderSideEnum.SELL);
                            io.bhex.base.account.BatchCancelOrderRequest.Builder builder = io.bhex.base.account.BatchCancelOrderRequest.newBuilder();
                            builder.setAccountId(account.getAccountId());
                            builder.setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header));
                            builder.addAllSides(sides);
                            if (symbols.size() > 0) {
                                builder.addAllSymbolIds(symbols);
                            }
                            grpcFuturesOrderService.batchCancelFuturesOrder(builder.build());

                           /* for (Order order : orders) {
                                try {
                                    cancelFuturesOrder(header, account.getAccountIndex(), order.getOrderId(), String.valueOf(clientIDGen.getAndIncrement()), PlanOrder.FuturesOrderType.LIMIT);
                                } catch (Exception e) {
                                    log.error("clearCancelByUser error orderId：{} accountId:{}", order.getOrderId(), account.getAccountId(), e);
                                }
                            }*/
                        } catch (Exception e) {
                            log.error("clear Cancel limit order error ", e);
                        } finally {
                            breakCount++;
                        }
                        Thread.sleep(100);
                    }
                    breakCount = 0;
                    while (breakCount <= 5) {
                        try {
                            //获取计划委托
                            List<PlanOrder.PlanOrderTypeEnum> planOrderTypes;
                            planOrderTypes = Lists.newArrayList(
                                    PlanOrder.PlanOrderTypeEnum.STOP_COMMON,
                                    PlanOrder.PlanOrderTypeEnum.STOP_LONG_PROFIT,
                                    PlanOrder.PlanOrderTypeEnum.STOP_LONG_LOSS,
                                    PlanOrder.PlanOrderTypeEnum.STOP_SHORT_PROFIT,
                                    PlanOrder.PlanOrderTypeEnum.STOP_SHORT_LOSS
                            );
                            List<PlanOrder> planOrders = queryFuturesPlanOrders(header, account.getAccountId(), "", 0L, 0L,
                                    0L, 0L, orderLimit, OrderQueryType.CURRENT, planOrderTypes);
                            if (planOrders.isEmpty()) {
                                break;
                            }
                            for (PlanOrder order : planOrders) {
                                try {
                                    cancelFuturesOrder(header, account.getAccountIndex(), order.getOrderId(), String.valueOf(clientIDGen.getAndIncrement()), PlanOrder.FuturesOrderType.STOP);
                                } catch (Exception e) {
                                    log.error("clearCancelByUser error orderId：{} accountId:{}", order.getOrderId(), account.getAccountId(), e);
                                }
                            }
                        } catch (Exception e) {
                            log.error("clear Cancel stop order error ", e);
                        } finally {
                            breakCount++;
                        }
                        Thread.sleep(100);
                    }
                }
            } catch (Exception e) {
                log.error(" clearCancelByUser error", e);
            }
        }, taskExecutor);
    }

    public void clearCloseByUser(Header header) {
        //获取所有合约子账户
        List<Account> accountList = accountService.querySubAccountList(header, AccountType.FUTURES);
        if (accountList.isEmpty()) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        CompletableFuture.runAsync(() -> {
            try {
                CommonIni commonIni = commonIniService.getCommonIni(header.getOrgId(), ORG_API_CLEAR_CLOSE);
                List<String> whiteSymbol = new ArrayList<>();
                if (commonIni != null) {
                    whiteSymbol = Arrays.asList(commonIni.getIniValue().split(","));
                }
                for (Account account : accountList) {
                    try {
                        List<FuturesPosition> positions = new ArrayList<>();
                        int size = 500;
                        Long endBalanceId = 1L;
                        while (size >= 500) {
                            io.bhex.base.account.FuturesPositionsRequest req = io.bhex.base.account.FuturesPositionsRequest.newBuilder()
                                    .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                                    .setAccountId(account.getAccountId())
                                    .setEndBalanceId(endBalanceId)
                                    .addAllTokenIds(new ArrayList<>())
                                    .setLimit(500)
                                    .setIsLong(-1)
                                    .build();

                            List<FuturesPosition> result = getBhexFuturesPositionsEx(header, req, account.getAccountId());
                            for (FuturesPosition position : result) {
                                if (endBalanceId.compareTo(position.getPositionId()) < 0) {
                                    endBalanceId = position.getPositionId();
                                }
                                if (whiteSymbol.contains(position.getTokenId()) && new BigDecimal(position.getAvailable()).compareTo(BigDecimal.ZERO) > 0) {
                                    positions.add(position);
                                }
                            }
                            size = result.size();
                        }
                        if (positions.isEmpty()) {
                            continue;
                        }
                        //根据持仓下对应的强平市价单
                        for (FuturesPosition position : positions) {
                            try {
                                SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(header.getOrgId(), position.getTokenId());
                                if (symbolDetail == null) {
                                    continue;
                                }
                                //重写exchangeId
                                long exchangeId = symbolDetail.getExchangeId();
                                FuturesOrderSide orderSide = FuturesOrderSide.SELL_CLOSE;
                                if (position.getIsLong().equals("0")) {//强平空仓
                                    orderSide = FuturesOrderSide.BUY_CLOSE;
                                }
                                String clientOrderId = String.valueOf(clientIDGen.getAndIncrement());
                                CreateFuturesOrderRequest request = CreateFuturesOrderRequest.newBuilder()
                                        .setHeader(header)
                                        .setExchangeId(exchangeId)
                                        .setSymbolId(symbolDetail.getSymbolId())
                                        .setClientOrderId(clientOrderId)
                                        .setFuturesOrderSide(orderSide)
                                        .setFuturesOrderType(PlanOrder.FuturesOrderType.LIMIT)
                                        .setPrice("0")
                                        .setQuantity(position.getAvailable())
                                        .setFuturesPriceType(FuturesPriceType.MARKET_PRICE)
                                        .setTriggerPrice("0")
                                        .setIsClose(true)
                                        .setLeverage("")
                                        .setTimeInForce(io.bhex.broker.grpc.order.OrderTimeInForceEnum.IOC)
                                        .setExtraFlag(FuturesOrderExtraFlag.UNKNOWN_FLAG)
                                        .setPlanOrderType(PlanOrder.PlanOrderTypeEnum.STOP_COMMON)
                                        .setTriggerCondition(PlanOrder.TriggerConditionEnum.CONDITION_REALTIME)
                                        .setOrderSource("")
                                        .build();
                                newFuturesOrder(request);
                            } catch (Exception e) {
                                log.error("clearCloseByUser new future order error", e);
                            }

                        }
                    } catch (Exception e) {
                        log.error("clearCloseByUser error", e);
                    }

                }

            } catch (Exception e) {
                log.error(" clearCloseByUser error", e);
            }
        }, taskExecutor);
    }
}

