/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.protobuf.TextFormat;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.account.BatchCancelOrderReply;
import io.bhex.base.account.BatchCancelOrderRequest;
import io.bhex.base.account.BatchCancelPlanSpotOrderReply;
import io.bhex.base.account.BatchCancelPlanSpotOrderRequest;
import io.bhex.base.account.CancelMatchOrderReply;
import io.bhex.base.account.CancelMatchOrderRequest;
import io.bhex.base.account.CancelOrderReply;
import io.bhex.base.account.CancelOrderRequest;
import io.bhex.base.account.CancelPlanSpotOrderReply;
import io.bhex.base.account.CancelPlanSpotOrderRequest;
import io.bhex.base.account.FastCancelOrderReply;
import io.bhex.base.account.FastCancelOrderRequest;
import io.bhex.base.account.GetBestOrderRequest;
import io.bhex.base.account.GetDepthInfoRequest;
import io.bhex.base.account.GetOrderReply;
import io.bhex.base.account.GetOrderRequest;
import io.bhex.base.account.GetOrderTradeDetailReply;
import io.bhex.base.account.GetOrderTradeDetailRequest;
import io.bhex.base.account.GetOrdersReply;
import io.bhex.base.account.GetOrdersRequest;
import io.bhex.base.account.GetPlanSpotOrderReply;
import io.bhex.base.account.GetPlanSpotOrdersReply;
import io.bhex.base.account.GetPlanSpotOrdersRequest;
import io.bhex.base.account.GetTradesReply;
import io.bhex.base.account.GetTradesRequest;
import io.bhex.base.account.NewOrderReply;
import io.bhex.base.account.NewOrderRequest;
import io.bhex.base.account.NewPlanSpotOrderReply;
import io.bhex.base.account.NewPlanSpotOrderRequest;
import io.bhex.base.account.Trade;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.margin.cross.GetMarginPositionStatusReply;
import io.bhex.base.margin.cross.MarginCrossPositionStatusEnum;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.proto.OrderStatusEnum;
import io.bhex.base.proto.OrderTimeInForceEnum;
import io.bhex.base.proto.OrderTypeEnum;
import io.bhex.base.rc.OrderSign;
import io.bhex.base.token.MakerBonusConfig;
import io.bhex.base.token.SymbolDetail;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteListType;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.order.BatchCancelOrderResponse;
import io.bhex.broker.grpc.order.BatchCancelPlanSpotOrderResponse;
import io.bhex.broker.grpc.order.CancelOrderResponse;
import io.bhex.broker.grpc.order.CancelPlanSpotOrderResponse;
import io.bhex.broker.grpc.order.CreateOrderResponse;
import io.bhex.broker.grpc.order.CreatePlanSpotOrderRequest;
import io.bhex.broker.grpc.order.CreatePlanSpotOrderResponse;
import io.bhex.broker.grpc.order.FastCancelOrderResponse;
import io.bhex.broker.grpc.order.Fee;
import io.bhex.broker.grpc.order.GetBanSellConfigResponse;
import io.bhex.broker.grpc.order.GetBestOrderResponse;
import io.bhex.broker.grpc.order.GetOrderBanConfigResponse;
import io.bhex.broker.grpc.order.GetOrderMatchResponse;
import io.bhex.broker.grpc.order.GetOrderResponse;
import io.bhex.broker.grpc.order.GetPlanSpotOrderRequest;
import io.bhex.broker.grpc.order.GetPlanSpotOrderResponse;
import io.bhex.broker.grpc.order.MatchInfo;
import io.bhex.broker.grpc.order.Order;
import io.bhex.broker.grpc.order.OrderQueryType;
import io.bhex.broker.grpc.order.OrderSide;
import io.bhex.broker.grpc.order.OrderType;
import io.bhex.broker.grpc.order.OrgBatchCancelOrderResponse;
import io.bhex.broker.grpc.order.PlanSpotOrder;
import io.bhex.broker.grpc.order.QueryMatchResponse;
import io.bhex.broker.grpc.order.QueryOrdersResponse;
import io.bhex.broker.grpc.order.QueryPlanSpotOrdersRequest;
import io.bhex.broker.grpc.order.QueryPlanSpotOrdersResponse;
import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.domain.TradeFeeConfig;
import io.bhex.broker.server.domain.UserBlackWhiteListConfig;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import io.bhex.broker.server.elasticsearch.service.ITradeDetailHistoryService;
import io.bhex.broker.server.grpc.client.service.GrpcOrderService;
import io.bhex.broker.server.grpc.server.service.aspect.SiteFunctionLimitSwitchAnnotation;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.OrderTokenHoldLimit;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.OrderTokenHoldLimitMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BigDecimalUtil;
import io.bhex.broker.server.util.GrpcRequestUtil;
import io.bhex.broker.server.util.SignUtils;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.util.StringUtil;

@Slf4j
@Service
public class OrderService {

    private static final Histogram COIN_ORDER_METRICS = Histogram.build()
            .namespace("broker")
            .subsystem("order")
            .name("coin_order_delay_milliseconds")
            .labelNames("process_name")
            .buckets(BrokerServerConstants.CONTROLLER_TIME_BUCKETS)
            .help("Histogram of stream handle latency in milliseconds")
            .register();

    public static final String SPECIAL_ORDER_TYPE_PERMISSION = "specialOrderType";

    @Resource
    private GrpcOrderService grpcOrderService;

    @Resource
    private AccountService accountService;

    @Autowired
    BasicService basicService;

    @Autowired
    MarketService marketService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private BrokerServerProperties brokerServerProperties;

    @Resource
    private ITradeDetailHistoryService tradeDetailHistoryService;

    @Resource
    private MarginService marginService;

    @Resource
    SpotQuoteService spotQuoteService;

    @Resource
    PushDataService pushDataService;

    @Resource
    private AccountMapper accountMapper;

    private static final BigDecimal MAX_PRICE_VALUE = new BigDecimal(99_999_999);
    private static final BigDecimal MAX_NUMBER = new BigDecimal(999_999_999);

    private static final BigDecimal DEFAULT_MAKER_ORDER_FEE = new BigDecimal("0.001");
    private static final BigDecimal DEFAULT_TAKER_ORDER_FEE = new BigDecimal("0.001");
    private static final TradeFeeConfig DEFAULT_TRADE_DEE_CONFIG = TradeFeeConfig.builder()
            .makerFeeRate(DEFAULT_MAKER_ORDER_FEE)
            .takerFeeRate(DEFAULT_TAKER_ORDER_FEE)
            .build();

    private static final BigDecimal DEFAULT_TRANSFER_MAKER_ORDER_FEE = new BigDecimal("0");
    private static final BigDecimal DEFAULT_TRANSFER_TAKER_ORDER_FEE = new BigDecimal("0.00025");
    private static final TradeFeeConfig DEFAULT_TRANSFER_TRADE_DEE_CONFIG = TradeFeeConfig.builder()
            .makerFeeRate(DEFAULT_TRANSFER_MAKER_ORDER_FEE)
            .takerFeeRate(DEFAULT_TRANSFER_TAKER_ORDER_FEE)
            .build();

    @Resource
    private SignUtils signUtils;

    @Resource
    private SymbolMapper symbolMapper;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private BrokerFeeService brokerFeeService;

    @Resource
    private UserBlackWhiteListConfigService userBlackWhiteListConfigService;

    @Resource
    private OrderTokenHoldLimitMapper orderTokenHoldLimitMapper;

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_COIN_TRADE_KEY,
            userSwitchGroupKey = BaseConfigConstants.FROZEN_USER_COIN_TRADE_GROUP)
    public BaseResult<CreateOrderResponse> newOrder(Header header, int accountIndex, String symbolId,
                                                    String clientOrderId, OrderType orderType, OrderSide orderSide, String price, String qty,
                                                    io.bhex.broker.grpc.order.OrderTimeInForceEnum timeInForceEnum, String orderSource,
                                                    AccountTypeEnum accountType) {
        if (header.getUserId() == 842145604923681280L) { //1.禁止这个子账户交易 7070的子账号
            return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED);
        }

        BrokerServerConstants.CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.SPOT_ORDER_TYPE, header.getPlatform().name()).inc();
        long startTimestamp = System.currentTimeMillis();

        if (header.getPlatform() == Platform.OPENAPI) {
            SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                    BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_API_COIN_KEY);
            if (switchStatus.isOpen()) {
                BaseConfigInfo baseConfigInfo = baseBizConfigService.getOneConfig(header.getOrgId(), BaseConfigConstants.OPEN_API_COIN_SYMBOL_WHITE_GROUP, symbolId);
                if (baseConfigInfo == null) {
                    return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED, "ApiNewOrder closed");
                }

                List<Long> configValues = Arrays.stream(baseConfigInfo.getConfValue().split(",")).map(Long::parseLong).collect(Collectors.toList());
                if (CollectionUtils.isEmpty(configValues) || !configValues.contains(header.getUserId())) {
                    log.error("ApiNewOrder closed and userId:{} is not on the white list", header.getUserId());
                    return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED, "ApiNewOrder closed");
                }
            }

            SwitchStatus userSwitchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                    BaseConfigConstants.FROZEN_USER_API_COIN_TRADE_GROUP, header.getUserId() + "");
            if (userSwitchStatus.isOpen()) {
                log.info("newOrderSuspended org:{} userId:{}", header.getOrgId(), header.getUserId());
                return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED, "ApiNewOrder closed");
            }
        }
        COIN_ORDER_METRICS.labels("openapiOrderCheck").observe(System.currentTimeMillis() - startTimestamp);
        long timestamp2 = System.currentTimeMillis();

        BaseBizConfigService.ConfigData allowSymbolsConfig = baseBizConfigService.getBrokerBaseConfig(header.getOrgId(),
                BaseConfigConstants.ALLOW_TRADING_SYMBOLS_GROUP,
                header.getUserId() + "", null);
        if (allowSymbolsConfig != null && StringUtils.isNoneEmpty(allowSymbolsConfig.getValue())) {
            List<String> allowSymbols = Lists.newArrayList(allowSymbolsConfig.getValue().split(","));
            if (!allowSymbols.contains(symbolId)) {
                log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot trade. not in allowSymbolList", header.getUserId(), symbolId);
                return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED);
            }
        }
        // 下单价格最小值判断
        // 下单价格精度判断
        // 下单数量最小值判断
        // 下单数量精度判断

//        //TODO YANTUSDT 买单直接抛异常 临时解决 后边币对就下掉了
//        if ("YANTUSDT".equalsIgnoreCase(symbolId) && orderSide == OrderSide.BUY) {
//            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
////            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
//        }
//
//        if (header.getOrgId() == 6002) {
//            if ("DRINKUSDT".equalsIgnoreCase(symbolId) && orderSide == OrderSide.BUY) {
//                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
////                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
//            }
//            if ("DRINKBTC".equalsIgnoreCase(symbolId) && orderSide == OrderSide.BUY) {
//                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
////                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
//            }
//        }

//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getMainAccountId(header);
        Long accountId;
        //杠杆账户判断当前账户是否处于强平
        if (accountType != null && accountType == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
            GetMarginPositionStatusReply positionStatusReply = marginService.getMarginPositionStatus(header, accountId);
            if (positionStatusReply.getCurStatus() == MarginCrossPositionStatusEnum.POSITION_FORCE_CLOSING) {
                log.warn("orgId:{} userId:{} accountId:{} status is FORCE_CLOSING cannot create order", header.getOrgId(), header.getUserId(), accountId);
                return BaseResult.fail(BrokerErrorCode.MARGIN_ACCOUNT_IS_FORCE_CLOSE);
            }

            SwitchStatus userSwitchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                    BaseConfigConstants.FROZEN_USER_MARGIN_TRADE_GROUP, header.getUserId() + "");
            if (userSwitchStatus.isOpen()) {
                log.info("newMarginOrderSuspended org:{} userId:{}", header.getOrgId(), header.getUserId());
                return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED, "forbidden");
            }
        } else {
            accountId = accountService.getMainIndexAccountId(header, accountIndex);
        }

        if (orderType == null || orderSide == null
                || orderType == OrderType.UNKNOWN_ORDER_TYPE || orderSide == OrderSide.UNKNOWN_ORDER_SIDE) {
            log.warn("orgId:{} userId:{} accountId:{} cid:{} create order cannot transfer orderType {}", header.getOrgId(), header.getUserId(), accountId, clientOrderId, orderType);
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
//            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        NewOrderRequest.Builder builder = NewOrderRequest.newBuilder();
        OrderSideEnum orderSideEnum = OrderSideEnum.valueOf(orderSide.name());


        BigDecimal quantity = new BigDecimal(qty).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        // TODO: 确认是否只有币币数据的推送
        io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), symbolId);
        if (symbol == null) {
            log.warn("orgId:{} userId:{} accountId:{} cid:{} create order cannot find symbol {}", header.getOrgId(), header.getUserId(), accountId, clientOrderId, symbolId);
            return BaseResult.fail(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
//            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        // 如果关闭了此币对的openapi交易功能，并且 accountId 不在 tb_account_trade_fee 表中缓存，那么提示 openapi 关闭此币对的交易
        if (header.getUserId() != 10000 && header.getPlatform() == Platform.OPENAPI && symbol.getForbidOpenapiTrade()
                && brokerFeeService.getCachedTradeFeeConfig(header.getOrgId(), symbol.getExchangeId(), symbol.getSymbolId(), accountId) == null) {
            return BaseResult.fail(BrokerErrorCode.SYMBOL_OPENAPI_TRADE_FORBIDDEN);
        }
//        if (header.getUserId() != 10000 && header.getOrgId() != 6004 && symbol.getIsAggragate()
//                && symbol.getMatchExchangeId() == 301 && header.getPlatform() == Platform.OPENAPI) {
//            return BaseResult.fail(BrokerErrorCode.SYMBOL_OPENAPI_TRADE_FORBIDDEN);
//        }

        //校验是否禁卖, 该用户不在白名单内禁卖
        if (orderSide == OrderSide.SELL && (symbol.getBanSellStatus() || !symbol.getBrokerAllowTrade())) {
            UserBlackWhiteListConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(header.getOrgId(), header.getUserId(),
                    UserBlackWhiteListType.SYMBOL_BAN_SELL_WHITE_LIST_TYPE_VALUE, UserBlackWhiteType.WHITE_CONFIG_VALUE);
            if (config == null || config.getStatus() == 0) {
                log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot sell. order:{}", header.getUserId(), symbolId, TextFormat.shortDebugString(builder.build()));
                return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
            }
        }

        //校验是否禁买, 该用户不在白名单内禁买
        if (orderSide == OrderSide.BUY && (symbol.getBanBuyStatus() || !symbol.getBrokerAllowTrade())) {
            UserBlackWhiteListConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(header.getOrgId(), header.getUserId(),
                    UserBlackWhiteListType.SYMBOL_BAN_BUY_WHITE_LIST_TYPE_VALUE, UserBlackWhiteType.WHITE_CONFIG_VALUE);
            if (config == null || config.getStatus() == 0) {
                log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot buy. order:{}", header.getUserId(), symbolId, TextFormat.shortDebugString(builder.build()));
                return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
            }
        }

        if (orderSide == OrderSide.BUY) {
            BaseBizConfigService.ConfigData disableBuySymbolsConfig = baseBizConfigService.getBrokerBaseConfig(header.getOrgId(),
                    BaseConfigConstants.DISABLE_USER_TRADING_SYMBOLS_GROUP, header.getUserId() + "-OPEN", null);
            if (disableBuySymbolsConfig != null && StringUtils.isNotEmpty(disableBuySymbolsConfig.getValue())) {
                List<String> disableSymbols = Lists.newArrayList(disableBuySymbolsConfig.getValue().split(","));
                if (disableSymbols.contains(symbolId)) {
                    log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot buy. in disableBuySymbolList", header.getUserId(), symbolId);
                    return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED);
                }
            }
        }

        if (orderSide == OrderSide.SELL) {
            BaseBizConfigService.ConfigData disableSellSymbolsConfig = baseBizConfigService.getBrokerBaseConfig(header.getOrgId(),
                    BaseConfigConstants.DISABLE_USER_TRADING_SYMBOLS_GROUP, header.getUserId() + "-CLOSE", null);
            if (disableSellSymbolsConfig != null && StringUtils.isNotEmpty(disableSellSymbolsConfig.getValue())) {
                List<String> disableSymbols = Lists.newArrayList(disableSellSymbolsConfig.getValue().split(","));
                if (disableSymbols.contains(symbolId)) {
                    log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot sell. in disableSellSymbolList", header.getUserId(), symbolId);
                    return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED);
                }
            }
        }

        if ((accountType != null && accountType == AccountTypeEnum.MARGIN) && !symbol.getAllowMargin()) {
            log.warn("orgId:{} symbol:{} cannot margin trade", header.getOrgId(), symbol.getSymbolId());
            return BaseResult.fail(BrokerErrorCode.MARGIN_SYMBOL_NOT_TRADE);
        }
        if (!symbol.getBrokerOnlineStatus() || !symbol.getExchangeAllowTrade()) {
            log.warn("SYMBOL_PROHIBIT_ORDER org:{} symbol:{} In a broker prohibited trading state.", header.getOrgId(), symbolId);
            return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
        }

        if (!symbol.getSaasAllowTrade()) {
            log.warn("SYMBOL_PROHIBIT_ORDER org:{} symbol:{} In a saas prohibited trading state.", header.getOrgId(), symbolId);
            return BaseResult.fail(BrokerErrorCode.SYMBOL_PLATFORM_PROHIBIT_ORDER);
        }
        COIN_ORDER_METRICS.labels("switchCheck").observe(System.currentTimeMillis() - timestamp2);
        long timestamp3 = System.currentTimeMillis();

        if (symbol.getCheckInPreview() && !brokerServerProperties.getIsPreview() && System.currentTimeMillis() < symbol.getOpenTime()) {
            log.warn("SYMBOL_PROHIBIT_ORDER org:{} symbol:{} not in open time {}", header.getOrgId(), symbolId, symbol.getOpenTime());
            return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
//            throw new BrokerException(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
        }
        if (orderType == OrderType.COM || orderType == OrderType.LOCAL_ONLY
                || orderType == OrderType.LIMIT_FREE || orderType == OrderType.LIMIT_MAKER_FREE) {
            if (header.getPlatform() != Platform.OPENAPI) {
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
//                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
            }
            if (!commonIniService.getStringValueOrDefault(header.getOrgId(), SPECIAL_ORDER_TYPE_PERMISSION, "").contains(String.valueOf(header.getUserId()))) {
                log.warn("{}-{} create {} {} order with cid:{}, but not in white list", header.getOrgId(), header.getUserId(), symbolId, orderType, clientOrderId);
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
//                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
            }
        }
        // 校验限价单的下单价格
        if (orderType != OrderType.MARKET) {
            // 价格校验   最大值、最小值、小数位数
            BaseResult<CreateOrderResponse> baseResult = validPrice(new BigDecimal(price), symbol);
            if (!baseResult.isSuccess()) {
                return baseResult;
            }
        }

        // 校验限价单的下单数量或者市价卖单的下单数量
        if (orderType != OrderType.MARKET || orderSide == OrderSide.SELL) {
            // 数量校验   最大值、最小值
            BaseResult<CreateOrderResponse> baseResult = validQuantity(quantity, symbol);
            if (!baseResult.isSuccess()) {
                return baseResult;
            }
        }

        // 校验限价单的 price * quantity 或者  市价买单的 quantity
        // 20190115变更：只限制市价买单
        if (orderType == OrderType.MARKET && orderSide == OrderSide.BUY) {
            // 市价买单的amount=quantity
            BaseResult<CreateOrderResponse> baseResult = validAmount(quantity, symbol);
            if (!baseResult.isSuccess()) {
                return baseResult;
            }
        }
        COIN_ORDER_METRICS.labels("paramCheck").observe(System.currentTimeMillis() - timestamp3);
        long timestamp4 = System.currentTimeMillis();

        // TODO: check etf order holdPosition
        // getBalance + queryOpenOrder(BUY)

        builder.setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setExchangeId(symbol.getExchangeId())
                .setSymbolId(symbol.getSymbolId())
                .setClientOrderId(clientOrderId)
                .setSide(orderSideEnum)
                .setPrice(DecimalUtil.fromBigDecimal(new BigDecimal(price)))
                .setTimeInForce(OrderTimeInForceEnum.valueOf(timeInForceEnum.name()));
        switch (orderType) {
            case LIMIT_MAKER:
                builder.setOrderType(OrderTypeEnum.LIMIT_MAKER);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LIMIT:
                builder.setOrderType(OrderTypeEnum.LIMIT);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case COM:
                builder.setOrderType(OrderTypeEnum.COM);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LOCAL_ONLY:
                builder.setOrderType(OrderTypeEnum.LOCAL_ONLY);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LIMIT_FREE:
                builder.setOrderType(OrderTypeEnum.LIMIT_FREE);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LIMIT_MAKER_FREE:
                builder.setOrderType(OrderTypeEnum.LIMIT_MAKER_FREE);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case MARKET:
                if (orderSideEnum == OrderSideEnum.BUY) {
                    builder.setOrderType(OrderTypeEnum.MARKET_OF_QUOTE);
                    builder.setAmount(DecimalUtil.fromBigDecimal(quantity));
                } else {
                    builder.setOrderType(OrderTypeEnum.MARKET_OF_BASE);
                    builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                }
                break;
            default:
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
//                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        // TODO
        BigDecimal tokenHoldLimitAmount = BigDecimal.ZERO;
        if (header.getOrgId() != 6004 && orderSide == OrderSide.BUY) {
            OrderTokenHoldLimit tokenHoldLimit = orderTokenHoldLimitMapper.selectOne(OrderTokenHoldLimit.builder().orgId(header.getOrgId()).symbolId(symbol.getSymbolId()).build());
            if (tokenHoldLimit != null && !tokenHoldLimit.getWhiteListUserId().contains(String.valueOf(header.getUserId()))) {
                tokenHoldLimitAmount = tokenHoldLimit.getHoldQuantity();
            }
        }
        builder.setPositionMaxLimit(DecimalUtil.fromBigDecimal(tokenHoldLimitAmount));

        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        builder.setSignTime(signTime).setSignNonce(signNonce);
        OrderSign orderSign = OrderSign.newBuilder()
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setExchangeId(symbol.getExchangeId())
                .setSymbolId(symbol.getSymbolId())
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

        String sign = "";
        try {
            sign = signUtils.sign(orderSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            log.error("create order add sign occurred a error orgId:{}  param:{}",
                    header.getOrgId(), TextFormat.shortDebugString(orderSign), e);
            return BaseResult.fail(BrokerErrorCode.ORDER_FAILED);
//            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
        builder.setSignTime(signTime).setSignNonce(signNonce).setSignBroker(sign);
        builder.setOrderSource(basicService.getOrderSource(header.getOrgId(), header.getPlatform(), orderSource));
        COIN_ORDER_METRICS.labels("orderSign").observe(System.currentTimeMillis() - timestamp4);
        long timestamp5 = System.currentTimeMillis();
        //校验精度位数
        BigDecimalUtil.checkParamScale(StringUtil.isNotEmpty(price) ? price : "", quantity != null ? quantity.toPlainString() : "");

        boolean isTransferOrder = false;
        if (symbol.getExchangeId() != symbol.getMatchExchangeId()) {
            isTransferOrder = Boolean.TRUE;
        }
        MakerBonusConfig config = basicService.getMakerBonusConfig(header.getOrgId(), symbol.getSymbolId());
        BigDecimal makerBonusRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMakerBonusRate());
        BigDecimal minInterestFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinInterestFeeRate());
        BigDecimal minTakerFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinTakerFeeRate());
//        log.info("getSpotOrderTransfer: orgId:{} userId:{}, accountId:{} symbolId:{} exchangeId:{}, matchExchangeId:{}", header.getOrgId(), header.getUserId(), accountId, symbolId, symbol.getExchangeId(), symbol.getMatchExchangeId());
        TradeFeeConfig tradeFeeConfig = brokerFeeService.getOrderTradeFeeConfig(header.getOrgId(), symbol.getExchangeId(), symbol.getSymbolId(), isTransferOrder, header.getUserId(), accountId, AccountType.MAIN,
                orderSide, DEFAULT_TRADE_DEE_CONFIG, DEFAULT_TRANSFER_TRADE_DEE_CONFIG, makerBonusRate, minInterestFeeRate, minTakerFeeRate);

        builder.setMakerFeeRate(DecimalUtil.fromBigDecimal(tradeFeeConfig.getMakerFeeRate()));
        builder.setTakerFeeRate(DecimalUtil.fromBigDecimal(tradeFeeConfig.getTakerFeeRate()));
        builder.setMakerBonusRate(tradeFeeConfig.getMakerFeeRate().compareTo(BigDecimal.ZERO) < 0 ?
                DecimalUtil.fromBigDecimal(makerBonusRate) : DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
        log.info("[Spot]handel broker fee, orgId:{} userId:{} accountId:{} clientOrderId:{} symbolId:{} makerFeeRate:{} takerFeeRate:{} makerBonusRate:{} minTakerFeeRate:{}",
                header.getOrgId(), header.getUserId(), accountId, clientOrderId, symbolId, builder.getMakerFeeRate().getStr(), builder.getTakerFeeRate().getStr(),
                makerBonusRate.toPlainString(), minTakerFeeRate.toPlainString());

        COIN_ORDER_METRICS.labels("feeRateSetting").observe(System.currentTimeMillis() - timestamp5);
        long timestamp6 = System.currentTimeMillis();
        NewOrderRequest request = builder.build();
        BaseResult<NewOrderReply> baseResult = grpcOrderService.createOrder(request);
        if (!baseResult.isSuccess()) {
            if (baseResult.getCode() == BrokerErrorCode.CREATE_ORDER_ERROR_REACH_HOLD_LIMIT.code()) {
                return BaseResult.fail(BrokerErrorCode.CREATE_ORDER_ERROR_REACH_HOLD_LIMIT, tokenHoldLimitAmount.stripTrailingZeros().toPlainString());
            }
            return BaseResult.fail(BrokerErrorCode.fromCode(baseResult.getCode()));
        }
        BrokerServerConstants.SUCCESS_CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.SPOT_ORDER_TYPE, header.getPlatform().name()).inc();
        NewOrderReply reply = baseResult.getData();
        BaseResult<CreateOrderResponse> orderResult = BaseResult.success(CreateOrderResponse.newBuilder().setOrder(getOrder(reply.getOrder())).build());
        COIN_ORDER_METRICS.labels("orderRequest").observe(System.currentTimeMillis() - timestamp6);
        COIN_ORDER_METRICS.labels("allProcess").observe(System.currentTimeMillis() - startTimestamp);
        //异步判断是否需要通知订单
        pushDataService.userSpotOrderNotifyMessage(header.getOrgId(), accountType, orderResult.getData().getOrder());
        return orderResult;
    }

    /**
     * price只校验直达之
     *
     * @param price
     * @param symbol
     */
    private BaseResult<CreateOrderResponse> validPrice(BigDecimal price, io.bhex.broker.grpc.basic.Symbol symbol) {
        if (price.compareTo(MAX_PRICE_VALUE) > 0) {
            log.warn("[order price max value check alert] validPrice price is bigger than MAX_NUMBER. price:{}, max_number:{}",
                    price.stripTrailingZeros().toPlainString(), MAX_NUMBER);
            return BaseResult.fail(BrokerErrorCode.ORDER_PRICE_TOO_HIGH);
//            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_TOO_HIGH);
        }
        if (price.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("[order price max value check alert] validPrice price is less than 0. price:{}",
                    price.stripTrailingZeros().toPlainString());
            return BaseResult.fail(BrokerErrorCode.ORDER_PRICE_TOO_SMALL);
//            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_TOO_SMALL);
        }
        if (price.remainder(new BigDecimal(symbol.getMinPricePrecision())).compareTo(BigDecimal.ZERO) != 0) {
            log.warn("[order price precision check alert] validPrice precision is larger than config. price:{}, minPricePrecision:{}",
                    price.stripTrailingZeros().toPlainString(), symbol.getMinPricePrecision());
            return BaseResult.fail(BrokerErrorCode.ORDER_PRICE_PRECISION_TOO_LONG);
//            throw new BrokerException(BrokerErrorCode.ORDER_PRICE_PRECISION_TOO_LONG);
        }
        return BaseResult.success();
    }

    public BaseResult<CreateOrderResponse> validQuantity(BigDecimal quantity, io.bhex.broker.grpc.basic.Symbol symbol) {
        if (quantity.compareTo(MAX_NUMBER) > 0) {
            log.warn("[order quantity max value check alert] validQuantity quantity is bigger than MAX_NUMBER. quantity:{}, max_number:{}",
                    quantity.stripTrailingZeros().toPlainString(), MAX_NUMBER);
            return BaseResult.fail(BrokerErrorCode.ORDER_QUANTITY_TOO_BIG);
//            throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_TOO_BIG);
        }
        if (quantity.compareTo(new BigDecimal(symbol.getMinTradeQuantity())) < 0) {
            log.warn("[order quantity min value check alert] validQuantity price is small than minTradeQuantity. quantity:{}, minTradeQuantity:{}",
                    quantity.stripTrailingZeros().toPlainString(), symbol.getMinTradeQuantity());
            return BaseResult.fail(BrokerErrorCode.ORDER_QUANTITY_TOO_SMALL);
//            throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_TOO_SMALL);
        }

        if (quantity.remainder(new BigDecimal(symbol.getBasePrecision())).compareTo(BigDecimal.ZERO) != 0) {
            log.warn("[order quantity precision check alert] validQuantity precision is larger than config. quantity:{}, minTradeQuantity:{}",
                    quantity.stripTrailingZeros().toPlainString(), symbol.getBasePrecision());
            return BaseResult.fail(BrokerErrorCode.ORDER_QUANTITY_PRECISION_TOO_LONG);
//            throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_PRECISION_TOO_LONG);
        }
        return BaseResult.success();
    }

    public BaseResult<CreateOrderResponse> validAmount(BigDecimal amount, io.bhex.broker.grpc.basic.Symbol symbol) {
        if (amount.compareTo(new BigDecimal(symbol.getMinTradeAmount())) < 0) {
            log.warn("[order amount min value check alert] validAmount:{} less than symbol required:{}.",
                    amount.toPlainString(), symbol.getMinTradeAmount());
            return BaseResult.fail(BrokerErrorCode.ORDER_AMOUNT_TOO_SMALL);
//            throw new BrokerException(BrokerErrorCode.ORDER_AMOUNT_TOO_SMALL);
        }
        if (amount.remainder(new BigDecimal(symbol.getQuotePrecision())).compareTo(BigDecimal.ZERO) != 0) {
            log.info("[order amount precision check alert] validAmount precision is larger than config. quantity:{}, minTradeQuantity:{}",
                    amount.stripTrailingZeros().toPlainString(), symbol.getQuotePrecision());
            return BaseResult.fail(BrokerErrorCode.ORDER_AMOUNT_PRECISION_TOO_LONG);
//            throw new BrokerException(BrokerErrorCode.ORDER_AMOUNT_PRECISION_TOO_LONG);
        }
        return BaseResult.success();
    }

    public GetOrderResponse getOrderInfo(Header header, AccountTypeEnum accountTypeEnum, int accountIndex, Long orderId, String clientOrderId) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getMainAccountId(header);
        Long accountId = accountService.getMainIndexAccountId(header, accountIndex);
        if (accountTypeEnum == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        }
        GetOrderRequest request = GetOrderRequest.newBuilder()
                .setAccountId(accountId)
                .setOrderId(orderId)
                .setClientOrderId(clientOrderId)
                .build();
        GetOrderReply reply = grpcOrderService.getOrderInfo(request);
        return GetOrderResponse.newBuilder().setOrder(getOrder(reply.getOrder())).build();
    }

    public BaseResult<CancelOrderResponse> cancelOrder(Header header, int accountIndex, Long orderId, String clientOrderId,
                                                       AccountTypeEnum accountType) {
        Long accountId = accountService.getMainIndexAccountId(header, accountIndex);
        if (accountType != null && accountType == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        }
        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        CancelOrderRequest.Builder builder = CancelOrderRequest.newBuilder()
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

        String sign = "";
        try {
            sign = signUtils.sign(orderSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            log.error("cancel order add sign occurred a error", e);
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FAILED);
//            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED);
        }
        builder.setSignTime(signTime)
                .setSignNonce(signNonce)
                .setSignBorker(sign);
        CancelOrderRequest request = builder.build();
        BaseResult<CancelOrderReply> baseResult = grpcOrderService.cancelOrder(request);
        if (!baseResult.isSuccess()) {
            return BaseResult.fail(BrokerErrorCode.fromCode(baseResult.getCode()));
        }
        CancelOrderReply reply = baseResult.getData();
        return BaseResult.success(CancelOrderResponse.newBuilder().setOrder(getOrder(reply.getOrder())).build());
    }

    public BaseResult<FastCancelOrderResponse> fastCancelOrder(Header header, Long orderId, String clientOrderId, String symbolId, Integer securityType, int accountIndex, AccountTypeEnum accountType) {
        Long accountId = 0L;
        Long matchExchangeId = 0L;
        if (securityType == 1) {
            accountId = accountService.getMainIndexAccountId(header, accountIndex);
            if (accountType != null && accountType == AccountTypeEnum.MARGIN) {
                accountId = accountService.getMarginIndexAccountId(header, accountIndex);
            }
            io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), symbolId);
            if (symbol != null) {
                matchExchangeId = symbol.getMatchExchangeId();
            }
        } else if (securityType == 3) {
            accountId = accountService.getFuturesIndexAccountId(header, accountIndex);
            SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(header.getOrgId(), symbolId);
            if (symbolDetail != null) {
                matchExchangeId = symbolDetail.getMatchExchangeId();
            }
        }
        if (accountId <= 0) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        if (matchExchangeId <= 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        FastCancelOrderRequest.Builder builder = FastCancelOrderRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountId)
                .setOrderId(orderId)
                .setClientOrderId(clientOrderId)
                .setSecurityType(securityType)
                .setMatchExchangeId(matchExchangeId)
                .setSymbolId(symbolId);
        FastCancelOrderRequest request = builder.build();
        BaseResult<FastCancelOrderReply> baseResult = grpcOrderService.fastCancelOrder(request);
        if (!baseResult.isSuccess()) {
            return BaseResult.fail(BrokerErrorCode.fromCode(baseResult.getCode()));
        }
        return BaseResult.success(FastCancelOrderResponse.newBuilder().setIsCancelled(baseResult.getData().getIsCancelled()).build());
    }

    public BatchCancelOrderResponse batchCancelOrder(Header header, AccountTypeEnum accountTypeEnum, int accountIndex,
                                                     List<String> symbolIds, OrderSide orderSide) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getMainAccountId(header);
        Long accountId = accountService.getMainIndexAccountId(header, accountIndex);
        if (accountTypeEnum != null && accountTypeEnum == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        }
        BatchCancelOrderRequest.Builder builder = BatchCancelOrderRequest.newBuilder().setAccountId(accountId);
        if (symbolIds != null && symbolIds.size() > 0) {
            builder.addAllSymbolIds(symbolIds);
        }
        if (orderSide != null && orderSide != OrderSide.UNKNOWN_ORDER_SIDE) {
            builder.addSides(OrderSideEnum.valueOf(orderSide.name()));
        }
        // todo: add sign
        BatchCancelOrderReply reply = grpcOrderService.batchCancelOrder(builder.build());
        return BatchCancelOrderResponse.getDefaultInstance();
    }

    public OrgBatchCancelOrderResponse orgBatchCancelOrder(Header header, String symbolId) {
        Symbol symbol = symbolMapper.getOrgSymbol(header.getOrgId(), symbolId);
        if (symbol == null) {
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        CancelMatchOrderRequest request = CancelMatchOrderRequest.newBuilder()
                .setExchangeId(symbol.getExchangeId())
                .setSymbolId(symbolId)
                .setBrokerId(header.getOrgId())
                .build();
        // todo: add sign
        CancelMatchOrderReply reply = grpcOrderService.orgBatchCancelOrder(request);
        CancelMatchOrderReply.CancelMatchOrderReplyCode code = reply.getCancelMatchOrderReplyCode();
        if (code == CancelMatchOrderReply.CancelMatchOrderReplyCode.HIGH_FREQ) {
            throw new BrokerException(BrokerErrorCode.REQUEST_TOO_FAST);
        } else if (code == CancelMatchOrderReply.CancelMatchOrderReplyCode.NOT_ALLOW) {
            log.warn("{} invoke batchCancelOrgOrder with symbol:{} and reply is NOT_ALLOW", header.getOrgId(), symbolId);
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        return OrgBatchCancelOrderResponse.getDefaultInstance();
    }

    public QueryOrdersResponse queryOrders(Header header, AccountTypeEnum accountTypeEnum, int accountIndex,
                                           String symbolId, Long fromOrderId, Long endOrderId, Long startTime, Long endTime,
                                           String baseTokenId, String quoteTokenId, OrderType orderType, OrderSide orderSide,
                                           Integer limit, OrderQueryType orderQueryType) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getMainAccountId(header);
//        Long accountId = accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex);

        Long accountId = accountService.getMainIndexAccountId(header, accountIndex);
        if (accountTypeEnum != null && accountTypeEnum == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        }
        List<Order> orderList = queryOrders(header, accountId,
                symbolId, fromOrderId, endOrderId, startTime, endTime,
                baseTokenId, quoteTokenId, orderType, orderSide, limit, orderQueryType);
        return QueryOrdersResponse.newBuilder().addAllOrders(orderList).build();
    }

    public QueryOrdersResponse queryAnyOrders(Header header, Long accountId, AccountTypeEnum accountTypeEnum, int accountIndex,
                                              String symbolId, Long fromOrderId, Long endOrderId, Long startTime, Long endTime,
                                              OrderType orderType, OrderSide orderSide, Integer limit, OrderQueryType orderQueryType) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getMainAccountId(header);
//        Long accountId = accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex);
        accountId = accountId > 0 ? accountId : accountService.getMainIndexAccountId(header, accountIndex);
        List<Order> orderList = queryOrders(header, accountId, symbolId, fromOrderId, endOrderId,
                startTime, endTime, null, null, orderType, orderSide, limit, orderQueryType);
        return QueryOrdersResponse.newBuilder().addAllOrders(orderList).build();
    }

    public List<Order> queryOrders(Header header, Long accountId,
                                   String symbolId, Long fromOrderId, Long endOrderId, Long startTime, Long endTime,
                                   String baseTokenId, String quoteTokenId, OrderType orderType, OrderSide orderSide,
                                   Integer limit, OrderQueryType queryType) {
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
        if (!Strings.isNullOrEmpty(symbolId)) {
            io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), symbolId);
            if (symbol != null) {
                symbolId = symbol.getSymbolId();
            }
        }
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == null || startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        GetOrdersRequest.Builder builder = GetOrdersRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
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
        if (orderType != null && orderType != OrderType.UNKNOWN_ORDER_TYPE) {
            if (orderType == OrderType.LIMIT) {
                builder.addType(OrderTypeEnum.LIMIT).addType(OrderTypeEnum.LIMIT_MAKER);
            } else {
                builder.addType(OrderTypeEnum.MARKET_OF_BASE).addType(OrderTypeEnum.MARKET_OF_QUOTE);
            }
        }
        if (orderType != null && orderSide != OrderSide.UNKNOWN_ORDER_SIDE) {
            builder.addSide(OrderSideEnum.valueOf(orderSide.name()));
        } else {
            builder.addSide(OrderSideEnum.BUY).addSide(OrderSideEnum.SELL);
        }
        GetOrdersRequest request = builder.build();
        GetOrdersReply reply = grpcOrderService.queryOrders(request);
        List<Order> orderList = Lists.newArrayList();
        if (reply.getOrdersList() != null && reply.getOrdersList().size() > 0) {
            reply.getOrdersList().forEach(order -> orderList.add(getOrder(order)));
        }

        if (fromOrderId == 0 && endOrderId > 0 && header.getPlatform() != Platform.OPENAPI) {
            return Lists.reverse(orderList);
        }
        return orderList;
    }

    private Order getOrder(io.bhex.base.account.Order order) {
        String statusCode = order.getStatus().name();
        List<Fee> fees = order.getTradeFeesList().stream()
                .map(fee -> Fee.newBuilder()
                        .setFeeTokenId(fee.getToken().getTokenId())
                        .setFeeTokenName(fee.getToken().getTokenName())
                        .setFee(DecimalUtil.toBigDecimal(fee.getFee()).stripTrailingZeros().toPlainString())
                        .build())
                .collect(Collectors.toList());
        OrderType orderType;
        switch (order.getType()) {
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

        return Order.newBuilder()
                .setAccountId(order.getAccountId())
                .setOrderId(order.getOrderId())
                .setClientOrderId(order.getClientOrderId())
                .setExchangeId(order.getExchangeId())
                .setSymbolId(order.getSymbol().getSymbolId())
                .setSymbolName(basicService.getSymbolName(order.getOrgId(), order.getSymbolId()))
                .setBaseTokenId(order.getSymbol().getBaseToken().getTokenId())
                .setBaseTokenName(basicService.getTokenName(order.getOrgId(), order.getSymbol().getBaseToken().getTokenId()))
                .setQuoteTokenId(order.getSymbol().getQuoteToken().getTokenId())
                .setQuoteTokenName(basicService.getTokenName(order.getOrgId(), order.getSymbol().getQuoteToken().getTokenId()))
                .setPrice(DecimalUtil.toBigDecimal(order.getPrice()).stripTrailingZeros().toPlainString())
                .setOrigQty(order.getType() == OrderTypeEnum.MARKET_OF_QUOTE
                        ? DecimalUtil.toBigDecimal(order.getAmount()).stripTrailingZeros().toPlainString()
                        : DecimalUtil.toBigDecimal(order.getQuantity()).stripTrailingZeros().toPlainString())
                .setExecutedQty(DecimalUtil.toBigDecimal(order.getExecutedQuantity()).stripTrailingZeros().toPlainString())
                .setExecutedAmount(DecimalUtil.toBigDecimal(order.getExecutedAmount()).stripTrailingZeros().toPlainString())
                .setAvgPrice(DecimalUtil.toBigDecimal(order.getAveragePrice()).stripTrailingZeros().toPlainString())
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(order.getSide().name()))
                .addAllFees(fees)
                .setStatusCode(statusCode)
                .setTime(order.getCreatedTime())
                .setLastUpdated(order.getUpdatedTime())
                .setTimeInForce(io.bhex.broker.grpc.order.OrderTimeInForceEnum.valueOf(order.getTimeInForce().name()))
                .build();
    }

    public QueryMatchResponse queryMatchInfo(Header header, AccountTypeEnum accountTypeEnum, int accountIndex, String symbolId, Long fromTraderId, Long endTradeId,
                                             Long startTime, Long endTime, Integer limit, OrderSide orderSide, boolean fromEsHistory) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getMainAccountId(header);
//        Long accountId = accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex);
        Long accountId = accountService.getMainIndexAccountId(header, accountIndex);
        if (accountTypeEnum != null && accountTypeEnum == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        }
        if (!Strings.isNullOrEmpty(symbolId)) {
            io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), symbolId);
            if (symbol != null) {
                symbolId = symbol.getSymbolId();
            }
        }
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == null || startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        GetTradesRequest.Builder builder = GetTradesRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
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
        // 对于只有endId查询的情况，先查询ES，后查询shard
        // 只有endId，shard返回的数据是正序排列，并且这种情况只有openapi需要的是正序，其他给的都是反序排列
        List<MatchInfo> matchInfoList = Lists.newArrayList();
        if (fromTraderId == 0 && endTradeId > 0) {
            // 先查询ES
            List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, 0L, symbolId, "", orderSideValue, startTime, endTime,
                    fromTraderId, endTradeId, limit, false);
            matchInfoList.addAll(tradeDetailList.stream().map(this::getMatchInfo).collect(Collectors.toList()));
            // 再查询shard
            if (tradeDetailList.size() < limit) {
                Long nextId = endTradeId;
                if (tradeDetailList.size() > 0) {
                    nextId = tradeDetailList.get(tradeDetailList.size() - 1).getTradeDetailId();
                }
                builder.setEndTradeId(nextId).setLimit(limit - tradeDetailList.size());
                GetTradesReply reply = grpcOrderService.queryMatchInfo(builder.build());
                matchInfoList.addAll(reply.getTradesList().stream().map(this::getMatchInfo).collect(Collectors.toList()));
            }
        } else {
            // 先查询shard
            GetTradesReply reply = grpcOrderService.queryMatchInfo(builder.build());
            matchInfoList.addAll(reply.getTradesList().stream().map(this::getMatchInfo).collect(Collectors.toList()));
            // 在查询ES
            if (reply.getTradesCount() < limit) {
                Long nextId = fromTraderId;
                if (reply.getTradesCount() > 0) {
                    nextId = reply.getTradesList().get(reply.getTradesCount() - 1).getTradeId();
                }
                List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, 0L, symbolId, "", orderSideValue, startTime, endTime,
                        nextId, endTradeId, limit - reply.getTradesCount(), false);
                matchInfoList.addAll(tradeDetailList.stream().map(this::getMatchInfo).collect(Collectors.toList()));
            }
        }
        if (fromTraderId == 0 && endTradeId > 0 && header.getPlatform() != Platform.OPENAPI) {
            return QueryMatchResponse.newBuilder().addAllMatch(Lists.reverse(matchInfoList)).build();
        }
        return QueryMatchResponse.newBuilder().addAllMatch(matchInfoList).build();
    }

    public GetOrderMatchResponse getOrderMatchInfo(Header header, AccountTypeEnum accountTypeEnum, int accountIndex, Long orderId, Long fromTraderId, Integer limit) {
//        Long accountId = accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex);
        /*
         * getMatchInfo现在无分页，顺序是正序
         */
        Long accountId = accountService.getMainIndexAccountId(header, accountIndex);
        if (accountTypeEnum != null && accountTypeEnum == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        }
        if (orderId <= 0) {
            return GetOrderMatchResponse.getDefaultInstance();
        }
        List<MatchInfo> matchInfoList = Lists.newArrayList();
        List<TradeDetail> tradeDetailList = Lists.newArrayList();
        try {
            tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, orderId, "", "", null,
                    0, 0, 0L, 0L, limit, false);
        } catch (Exception e) {
            log.warn("getOrderMatchInfo error by ES orgId:{},userId:{}", header.getOrgId(), header.getUserId(), e.getMessage(), e);
        }

        if (!CollectionUtils.isEmpty(tradeDetailList)) {
            matchInfoList.addAll(tradeDetailList.stream().map(this::getMatchInfo).collect(Collectors.toList()));
        } else {
            GetOrderTradeDetailRequest request = GetOrderTradeDetailRequest.newBuilder()
                    .setAccountId(accountId)
                    .setOrderId(orderId)
                    .setFromTradeId(fromTraderId)
                    .setLimit(limit)
                    .build();
            GetOrderTradeDetailReply reply = grpcOrderService.getMatchInfo(request);
            matchInfoList.addAll(reply.getTradesList().stream().map(this::getMatchInfo).collect(Collectors.toList()));
        }
        return GetOrderMatchResponse.newBuilder().addAllMatch(matchInfoList).build();
    }

    public GetBanSellConfigResponse getBanSellConfig(Long userId, Long orgId, String symbolId) {

        Symbol symbol = symbolMapper.getOrgSymbol(orgId, symbolId);
        if (symbol == null) {
            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
        }

        if (symbol.getBanSellStatus() == null) {
            log.error("banSellStatus cant null");
            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
        }

        if (symbol.getBanSellStatus() == 0) {
            return GetBanSellConfigResponse.newBuilder().setCanSell(true).build();
        }

        if (symbol.getBanSellStatus() == 1) {

            UserBlackWhiteListConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(orgId, userId,
                    UserBlackWhiteListType.SYMBOL_BAN_SELL_WHITE_LIST_TYPE_VALUE, UserBlackWhiteType.WHITE_CONFIG_VALUE);

            if (config == null || config.getStatus() == 0) {
                return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
            } else {
                return GetBanSellConfigResponse.newBuilder().setCanSell(true).build();
            }
        } else {
            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
        }
    }

    public GetOrderBanConfigResponse getOrderBanConfig(Long userId, Long orgId, String symbolId) {

        Symbol symbol = symbolMapper.getOrgSymbol(orgId, symbolId);
        if (symbol == null) {
            return GetOrderBanConfigResponse.newBuilder().setCanBuy(false).setCanSell(false).build();
        }

        if (symbol.getBanSellStatus() == null || symbol.getBanBuyStatus() == null) {
            log.error("BanStatus cant null");
            return GetOrderBanConfigResponse.newBuilder().setCanBuy(false).setCanSell(false).build();
        }

        boolean canSell = false;
        boolean canBuy = false;
        if (symbol.getBanSellStatus() == 1) {
            UserBlackWhiteListConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(orgId, userId,
                    UserBlackWhiteListType.SYMBOL_BAN_SELL_WHITE_LIST_TYPE_VALUE, UserBlackWhiteType.WHITE_CONFIG_VALUE);
            if (config != null && config.getStatus() == 1) {
                canSell = true;
            }
        } else {
            canSell = true;
        }

        if (symbol.getBanBuyStatus() == 1) {
            UserBlackWhiteListConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(orgId, userId,
                    UserBlackWhiteListType.SYMBOL_BAN_BUY_WHITE_LIST_TYPE_VALUE, UserBlackWhiteType.WHITE_CONFIG_VALUE);
            if (config != null && config.getStatus() == 1) {
                canBuy = true;
            }
        } else {
            canBuy = true;
        }

        return GetOrderBanConfigResponse.newBuilder().setCanSell(canSell).setCanBuy(canBuy).build();
    }


//    public GetBanSellConfigResponse getBanSellConfig(Long userId, Long orgId, String symbolId) {
//        Long accountId = this.accountService.getAccountId(orgId, userId);
//        if (accountId == null) {
//            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
//        }
//        Symbol symbol = symbolMapper.getOrgSymbol(orgId, symbolId);
//        if (symbol == null) {
//            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
//        }
//
//        if (symbol.getBanSellStatus() == null) {
//            log.error("banSellStatus cant null");
//            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
//        }
//
//        if (symbol.getBanSellStatus() == 0) {
//            return GetBanSellConfigResponse.newBuilder().setCanSell(true).build();
//        }
//
//        if (symbol.getBanSellStatus() == 1) {
//            List<BanSellConfig> banSellConfigs
//                    = this.banSellConfigMapper.queryAllByOrgId(orgId);
//            //校验是否在白名单
//            if (banSellConfigs == null || banSellConfigs.size() == 0) {
//                return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
//            }
//            BanSellConfig sale = banSellConfigs.stream()
//                    .filter(saleWhite -> saleWhite.getAccountId().longValue() == accountId.longValue())
//                    .findFirst()
//                    .orElse(null);
//            if (sale == null) {
//                return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
//            } else {
//                return GetBanSellConfigResponse.newBuilder().setCanSell(true).build();
//            }
//        } else {
//            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
//        }
//    }

    private MatchInfo getMatchInfo(Trade trade) {
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
                .setPrice(DecimalUtil.toBigDecimal(trade.getPrice()).stripTrailingZeros().toPlainString())
                .setQuantity(DecimalUtil.toBigDecimal(trade.getQuantity()).stripTrailingZeros().toPlainString())
                .setAmount(DecimalUtil.toBigDecimal(trade.getQuantity()).stripTrailingZeros().toPlainString())
                .setFee(Fee.newBuilder()
                        .setFeeTokenId(trade.getTradeFee().getToken().getTokenId())
                        .setFeeTokenName(trade.getTradeFee().getToken().getTokenName())
                        .setFee(DecimalUtil.toBigDecimal(trade.getTradeFee().getFee()).stripTrailingZeros().toPlainString())
                        .build()
                )
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(trade.getSide().name()))
                .setTime(trade.getMatchTime())
                .setMatchOrgId(trade.getOrgId() == trade.getMatchOrgId() ? trade.getMatchOrgId() : 0)
                .setMatchUserId(trade.getOrgId() == trade.getMatchOrgId() ? Long.parseLong(trade.getMatchBrokerUserId()) : 0)
                .setMatchAccountId(trade.getMatchAccountId())
                .setIsMaker(trade.getIsMaker())
                .setIsNormal(trade.getIsNormal())
                .build();
    }

    private MatchInfo getMatchInfo(TradeDetail trade) {
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
        return MatchInfo.newBuilder()
                .setAccountId(trade.getAccountId())
                .setOrderId(trade.getOrderId())
                .setMatchOrderId(trade.getMatchOrderId())
                .setTradeId(trade.getTradeDetailId())
                .setSymbolId(trade.getSymbolId())
                .setSymbolName(basicService.getSymbolName(trade.getOrgId(), trade.getSymbolId()))
                .setBaseTokenId(trade.getBaseTokenId())
                .setBaseTokenName(basicService.getTokenName(trade.getOrgId(), trade.getBaseTokenId()))
                .setQuoteTokenId(trade.getQuoteTokenId())
                .setQuoteTokenName(basicService.getTokenName(trade.getOrgId(), trade.getQuoteTokenId()))
                .setPrice(trade.getPrice().stripTrailingZeros().toPlainString())
                .setQuantity(trade.getQuantity().stripTrailingZeros().toPlainString())
                .setAmount(trade.getAmount().stripTrailingZeros().toPlainString())
                .setFee(Fee.newBuilder()
                        .setFeeTokenId(trade.getFeeToken())
                        .setFeeTokenName(basicService.getTokenName(trade.getOrgId(), trade.getFeeToken()))
                        .setFee(trade.getFee().stripTrailingZeros().toPlainString())
                        .build()
                )
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(OrderSideEnum.forNumber(trade.getOrderSide()).name()))
//                .setTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(trade.getMatchTime(), new ParsePosition(1)).getTime())
                .setTime(trade.getMatchTime().getTime())
                .setMatchOrgId(trade.getMatchOrgId() != null && trade.getOrgId().equals(trade.getMatchOrgId()) ? trade.getMatchOrgId() : 0)
                .setMatchUserId(trade.getMatchOrgId() != null && trade.getMatchUserId() != null && trade.getOrgId().equals(trade.getMatchOrgId()) ? trade.getMatchUserId() : 0)
                .setMatchAccountId(trade.getMatchAccountId() != null ? trade.getMatchAccountId() : 0)
                .setIsMaker(trade.getIsMaker() == 1)
                .setIsNormal(trade.getAccountId().equals(trade.getMatchAccountId()))
                .build();
    }

    public GetBestOrderResponse getBestOrder(Header header, Long exchangeId, String symbolId) {
        Long accountId = accountService.getMainAccountId(header);
        GetBestOrderRequest request = GetBestOrderRequest.newBuilder()
                .setAccountId(accountId)
                .setExchangeId(exchangeId)
                .setSymbolId(symbolId)
                .build();
        io.bhex.base.account.GetBestOrderResponse response = grpcOrderService.getBestOrder(request);
        return GetBestOrderResponse.newBuilder()
                .setPrice(response.getPrice())
                .setBid(getOrder(response.getBid()))
                .setAsk(getOrder(response.getAsk()))
                .build();
    }

    public io.bhex.broker.grpc.order.GetDepthInfoResponse getDepthInfo(Header header, Long exchangeId, List<String> symbolIds) {
        Long accountId = accountService.getMainAccountId(header);
        GetDepthInfoRequest request = GetDepthInfoRequest.newBuilder()
                .setExchangeId(exchangeId)
                .addAllSymbols(symbolIds)
                .build();
        io.bhex.base.account.GetDepthInfoResponse response = grpcOrderService.getDepthInfo(request);
        return io.bhex.broker.grpc.order.GetDepthInfoResponse.newBuilder()
                .setTime(response.getTime())
                .setLevel(response.getLevel())
                .setExchangeId(response.getExchangeId())
                .addAllDepthInfo(response.getDepthList().stream()
                        .map(depthInfo -> io.bhex.broker.grpc.order.GetDepthInfoResponse.DepthInfo.newBuilder()
                                .setSymbolId(depthInfo.getSymbol())
                                .addAllAsk(depthInfo.getAsksList().stream()
                                        .map(ask -> io.bhex.broker.grpc.order.GetDepthInfoResponse.DepthInfo.OrderInfoList.newBuilder()
                                                .setPrice(DecimalUtil.toBigDecimal(ask.getPrice()).stripTrailingZeros().toPlainString())
                                                .setOriginalPrice(DecimalUtil.toBigDecimal(ask.getOriginalPrice()).stripTrailingZeros().toPlainString())
                                                .addAllOrderInfo(ask.getOrderInfoList().stream()
                                                        .map(order -> io.bhex.broker.grpc.order.GetDepthInfoResponse.DepthInfo.OrderInfoList.OrderInfo.newBuilder()
                                                                .setQuantity(DecimalUtil.toBigDecimal(order.getQuantity()).stripTrailingZeros().toPlainString())
                                                                .setAccountId(order.getAccountId())
                                                                .build()).collect(Collectors.toList()))
                                                .build())
                                        .collect(Collectors.toList()))
                                .addAllBid(depthInfo.getBidsList().stream()
                                        .map(bid -> io.bhex.broker.grpc.order.GetDepthInfoResponse.DepthInfo.OrderInfoList.newBuilder()
                                                .setPrice(DecimalUtil.toBigDecimal(bid.getPrice()).stripTrailingZeros().toPlainString())
                                                .setOriginalPrice(DecimalUtil.toBigDecimal(bid.getOriginalPrice()).stripTrailingZeros().toPlainString())
                                                .addAllOrderInfo(bid.getOrderInfoList().stream()
                                                        .map(order -> io.bhex.broker.grpc.order.GetDepthInfoResponse.DepthInfo.OrderInfoList.OrderInfo.newBuilder()
                                                                .setQuantity(DecimalUtil.toBigDecimal(order.getQuantity()).stripTrailingZeros().toPlainString())
                                                                .setAccountId(order.getAccountId())
                                                                .build()).collect(Collectors.toList()))
                                                .build())
                                        .collect(Collectors.toList()))
                                .build()).collect(Collectors.toList()))
                .build();
    }

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_COIN_TRADE_KEY,
            userSwitchGroupKey = BaseConfigConstants.FROZEN_USER_COIN_TRADE_GROUP)
    public BaseResult<CreatePlanSpotOrderResponse> createPlanSpotOrder(CreatePlanSpotOrderRequest request) {
        Header header = request.getHeader();
        //检查开关
        BrokerServerConstants.CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.SPOT_ORDER_TYPE, header.getPlatform().name()).inc();
        long startTimestamp = System.currentTimeMillis();
        if (header.getPlatform() == Platform.OPENAPI) {
            SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                    BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_API_COIN_KEY);
            if (switchStatus.isOpen()) {
                return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED);
            }

            SwitchStatus userSwitchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                    BaseConfigConstants.FROZEN_USER_API_COIN_TRADE_GROUP, header.getUserId() + "");
            if (userSwitchStatus.isOpen()) {
                log.info("newOrderSuspended org:{} userId:{}", header.getOrgId(), header.getUserId());
                return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED);
            }
        }
        COIN_ORDER_METRICS.labels("openapiOrderCheck").observe(System.currentTimeMillis() - startTimestamp);
        long timestamp2 = System.currentTimeMillis();

        //获取用户accountId,账户类型校验
        int accountIndex = request.getAccountIndex();
        AccountTypeEnum accountType = request.getAccountType();
        Long accountId;
        //杠杆账户判断当前账户是否处于强平, 强平不允许下任何订单
        if (accountType == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
            GetMarginPositionStatusReply positionStatusReply = marginService.getMarginPositionStatus(header, accountId);
            if (positionStatusReply.getCurStatus() == MarginCrossPositionStatusEnum.POSITION_FORCE_CLOSING) {
                log.warn("orgId:{} userId:{} accountId:{} status is FORCE_CLOSING cannot create order", header.getOrgId(), header.getUserId(), accountId);
                return BaseResult.fail(BrokerErrorCode.MARGIN_ACCOUNT_IS_FORCE_CLOSE);
            }
        } else {
            accountId = accountService.getMainIndexAccountId(header, accountIndex);
        }
        //检查入参
        String clientOrderId = request.getClientOrderId();
        OrderType orderType = request.getOrderType();
        OrderSide orderSide = request.getOrderSide();
        if (orderType == null || orderSide == null
                || orderType == OrderType.UNKNOWN_ORDER_TYPE || orderSide == OrderSide.UNKNOWN_ORDER_SIDE) {
            log.warn("orgId:{} userId:{} accountId:{} cid:{} create order cannot transfer orderType {}", header.getOrgId(), header.getUserId(), accountId, clientOrderId, orderType);
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }
        //校验币对是否存在
        String symbolId = request.getSymbolId();
        io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), symbolId);
        if (symbol == null) {
            log.warn("orgId:{} userId:{} accountId:{} cid:{} create order cannot find symbol {}", header.getOrgId(), header.getUserId(), accountId, clientOrderId, symbolId);
            return BaseResult.fail(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }

        //检查是否禁止计划委托
        if (!symbol.getAllowPlan()) {
            log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot allow plan. order:{}", header.getUserId(), symbolId, request.getClientOrderId());
            return BaseResult.fail(BrokerErrorCode.ORDER_PLAN_ALLOW_LIMIT);
        }
        NewPlanSpotOrderRequest.Builder builder = NewPlanSpotOrderRequest.newBuilder();
        //校验是否禁卖, 该用户不在白名单内禁卖
        if (orderSide == OrderSide.SELL && (symbol.getBanSellStatus() || !symbol.getBrokerAllowTrade())) {
            UserBlackWhiteListConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(header.getOrgId(), header.getUserId(),
                    UserBlackWhiteListType.SYMBOL_BAN_SELL_WHITE_LIST_TYPE_VALUE, UserBlackWhiteType.WHITE_CONFIG_VALUE);
            if (config == null || config.getStatus() == 0) {
                log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot sell. order:{}", header.getUserId(), symbolId, TextFormat.shortDebugString(builder.build()));
                return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
            }
        }

        //校验是否禁买, 该用户不在白名单内禁买
        if (orderSide == OrderSide.BUY && (symbol.getBanBuyStatus() || !symbol.getBrokerAllowTrade())) {
            UserBlackWhiteListConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(header.getOrgId(), header.getUserId(),
                    UserBlackWhiteListType.SYMBOL_BAN_BUY_WHITE_LIST_TYPE_VALUE, UserBlackWhiteType.WHITE_CONFIG_VALUE);
            if (config == null || config.getStatus() == 0) {
                log.warn("SYMBOL_PROHIBIT_ORDER user:{} symbol:{} cannot buy. order:{}", header.getUserId(), symbolId, TextFormat.shortDebugString(builder.build()));
                return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
            }
        }

        if ((accountType == AccountTypeEnum.MARGIN) && !symbol.getAllowMargin()) {
            log.warn("orgId:{} symbol:{} cannot margin trade", header.getOrgId(), symbol.getSymbolId());
            return BaseResult.fail(BrokerErrorCode.MARGIN_SYMBOL_NOT_TRADE);
        }
        if (!symbol.getBrokerOnlineStatus() || !symbol.getExchangeAllowTrade()) {
            log.warn("SYMBOL_PROHIBIT_ORDER org:{} symbol:{} In a broker prohibited trading state.", header.getOrgId(), symbolId);
            return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
        }

        if (!symbol.getSaasAllowTrade()) {
            log.warn("SYMBOL_PROHIBIT_ORDER org:{} symbol:{} In a saas prohibited trading state.", header.getOrgId(), symbolId);
            return BaseResult.fail(BrokerErrorCode.SYMBOL_PLATFORM_PROHIBIT_ORDER);
        }
        COIN_ORDER_METRICS.labels("switchCheck").observe(System.currentTimeMillis() - timestamp2);
        long timestamp3 = System.currentTimeMillis();

        if (symbol.getCheckInPreview() && !brokerServerProperties.getIsPreview() && System.currentTimeMillis() < symbol.getOpenTime()) {
            log.warn("SYMBOL_PROHIBIT_ORDER org:{} symbol:{} not in open time {}", header.getOrgId(), symbolId, symbol.getOpenTime());
            return BaseResult.fail(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
        }
        if (orderType == OrderType.COM || orderType == OrderType.LOCAL_ONLY
                || orderType == OrderType.LIMIT_FREE || orderType == OrderType.LIMIT_MAKER_FREE) {
            if (header.getPlatform() != Platform.OPENAPI) {
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
            }
            if (!commonIniService.getStringValueOrDefault(header.getOrgId(), SPECIAL_ORDER_TYPE_PERMISSION, "").contains(String.valueOf(header.getUserId()))) {
                log.warn("{}-{} create {} {} order with cid:{}, but not in white list", header.getOrgId(), header.getUserId(), symbolId, orderType, request.getClientOrderId());
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
            }
        }
        //校验触发价格
        String triggerPrice = request.getTriggerPrice();
        // 价格校验   最大值、最小值、小数位数
        BaseResult<CreateOrderResponse> checkTriggerPriceBaseResult = validPrice(new BigDecimal(triggerPrice), symbol);
        if (!checkTriggerPriceBaseResult.isSuccess()) {
            return BaseResult.<CreatePlanSpotOrderResponse>builder().success(false)
                    .code(checkTriggerPriceBaseResult.getCode()).msg(checkTriggerPriceBaseResult.getMsg())
                    .extendInfo(checkTriggerPriceBaseResult.getExtendInfo()).build();
        }
        // 校验限价单的下单价格
        String price = request.getPrice();
        if (orderType != OrderType.MARKET) {
            // 价格校验   最大值、最小值、小数位数
            BaseResult<CreateOrderResponse> baseResult = validPrice(new BigDecimal(price), symbol);
            if (!baseResult.isSuccess()) {
                return BaseResult.<CreatePlanSpotOrderResponse>builder().success(false)
                        .code(baseResult.getCode()).msg(baseResult.getMsg())
                        .extendInfo(baseResult.getExtendInfo()).build();
            }
        }
        // 校验限价单的下单数量或者市价卖单的下单数量
        BigDecimal quantity = new BigDecimal(request.getQuantity()).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        if (orderType != OrderType.MARKET || orderSide == OrderSide.SELL) {
            // 数量校验   最大值、最小值
            BaseResult<CreateOrderResponse> baseResult = validQuantity(quantity, symbol);
            if (!baseResult.isSuccess()) {
                return BaseResult.<CreatePlanSpotOrderResponse>builder().success(false)
                        .code(baseResult.getCode()).msg(baseResult.getMsg())
                        .extendInfo(baseResult.getExtendInfo()).build();
            }
        }

        // 校验限价单的 price * quantity 或者  市价买单的 quantity
        // 20190115变更：只限制市价买单
        if (orderType == OrderType.MARKET && orderSide == OrderSide.BUY) {
            // 市价买单的amount=quantity
            BaseResult<CreateOrderResponse> baseResult = validAmount(quantity, symbol);
            if (!baseResult.isSuccess()) {
                return BaseResult.<CreatePlanSpotOrderResponse>builder().success(false)
                        .code(baseResult.getCode()).msg(baseResult.getMsg())
                        .extendInfo(baseResult.getExtendInfo()).build();
            }
        }
        COIN_ORDER_METRICS.labels("paramCheck").observe(System.currentTimeMillis() - timestamp3);
        long timestamp4 = System.currentTimeMillis();
        OrderSideEnum orderSideEnum = OrderSideEnum.valueOf(request.getOrderSide().name());
        builder.setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountType(accountType.getNumber())
                .setAccountId(accountId)
                .setExchangeId(symbol.getExchangeId())
                .setSymbolId(symbol.getSymbolId())
                .setClientOrderId(clientOrderId)
                .setSide(orderSideEnum)
                .setBaseTokenId(symbol.getBaseTokenId())
                .setQuoteTokenId(symbol.getQuoteTokenId())
                .setTriggerPrice(DecimalUtil.fromBigDecimal(new BigDecimal(triggerPrice)))
                .setPrice(DecimalUtil.fromBigDecimal(new BigDecimal(price)))
                .setTimeInForce(OrderTimeInForceEnum.valueOf(request.getTimeInForce().name()));
        switch (orderType) {
            case LIMIT_MAKER:
                builder.setOrderType(OrderTypeEnum.LIMIT_MAKER);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LIMIT:
                builder.setOrderType(OrderTypeEnum.LIMIT);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case COM:
                builder.setOrderType(OrderTypeEnum.COM);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LOCAL_ONLY:
                builder.setOrderType(OrderTypeEnum.LOCAL_ONLY);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LIMIT_FREE:
                builder.setOrderType(OrderTypeEnum.LIMIT_FREE);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case LIMIT_MAKER_FREE:
                builder.setOrderType(OrderTypeEnum.LIMIT_MAKER_FREE);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            case MARKET:
                builder.setPrice(DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
                if (orderSideEnum == OrderSideEnum.BUY) {
                    builder.setOrderType(OrderTypeEnum.MARKET_OF_QUOTE);
                    builder.setAmount(DecimalUtil.fromBigDecimal(quantity));
                } else {
                    builder.setOrderType(OrderTypeEnum.MARKET_OF_BASE);
                    builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                }
                break;
            default:
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }
        //校验用户最大资产是否满足
        String tokenId;
        BigDecimal checkQuantity;
        if (orderSideEnum == OrderSideEnum.BUY && Objects.equals(orderType, OrderType.MARKET)) {
            tokenId = symbol.getQuoteTokenId();
            checkQuantity = quantity;
        } else if (orderSideEnum == OrderSideEnum.BUY) {
            tokenId = symbol.getQuoteTokenId();
            checkQuantity = new BigDecimal(price).multiply(quantity);
        } else {
            //卖都是base的量
            tokenId = symbol.getBaseTokenId();
            checkQuantity = quantity;
        }
        BigDecimal total = queryTokenIdTotalQuantity(header, accountId, tokenId);
        if (checkQuantity.compareTo(total) > 0) {
            return BaseResult.fail(BrokerErrorCode.ORDER_PLAN_TOTAL_BALANCE_LIMIT);
        }
        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        builder.setSignTime(signTime).setSignNonce(signNonce);
        OrderSign orderSign = OrderSign.newBuilder()
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setExchangeId(symbol.getExchangeId())
                .setSymbolId(symbol.getSymbolId())
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
        String sign = "";
        try {
            sign = signUtils.sign(orderSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            log.error("create order add sign occurred a error orgId:{}  param:{}",
                    header.getOrgId(), TextFormat.shortDebugString(orderSign), e);
            return BaseResult.fail(BrokerErrorCode.ORDER_FAILED);
        }
        builder.setSignTime(signTime).setSignNonce(signNonce).setSignBroker(sign);
        builder.setOrderSource(basicService.getOrderSource(header.getOrgId(), header.getPlatform(), request.getOrderSource()));
        COIN_ORDER_METRICS.labels("orderSign").observe(System.currentTimeMillis() - timestamp4);

        long timestamp5 = System.currentTimeMillis();
        //校验精度位数
        BigDecimalUtil.checkParamScale(StringUtil.isNotEmpty(price) ? price : "", quantity.toPlainString());
        // 共享深度的转移单
        boolean isTransferOrder = false;
        if (symbol.getExchangeId() != symbol.getMatchExchangeId()) {
            isTransferOrder = Boolean.TRUE;
        }
        MakerBonusConfig config = basicService.getMakerBonusConfig(header.getOrgId(), symbol.getSymbolId());
        BigDecimal makerBonusRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMakerBonusRate());
        BigDecimal minInterestFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinInterestFeeRate());
        BigDecimal minTakerFeeRate = config == null ? BigDecimal.ZERO : new BigDecimal(config.getMinTakerFeeRate());
        TradeFeeConfig tradeFeeConfig = brokerFeeService.getOrderTradeFeeConfig(header.getOrgId(), symbol.getExchangeId(), symbol.getSymbolId(), isTransferOrder, header.getUserId(), accountId, AccountType.MAIN,
                orderSide, DEFAULT_TRADE_DEE_CONFIG, DEFAULT_TRANSFER_TRADE_DEE_CONFIG, makerBonusRate, minInterestFeeRate, minTakerFeeRate);

        builder.setMakerFeeRate(DecimalUtil.fromBigDecimal(tradeFeeConfig.getMakerFeeRate()));
        builder.setTakerFeeRate(DecimalUtil.fromBigDecimal(tradeFeeConfig.getTakerFeeRate()));
        builder.setMakerBonusRate(tradeFeeConfig.getMakerFeeRate().compareTo(BigDecimal.ZERO) < 0 ?
                DecimalUtil.fromBigDecimal(makerBonusRate) : DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
        log.info("[Spot]handel broker fee, orgId:{} userId:{} accountId:{} clientOrderId:{} symbolId:{} makerFeeRate:{} takerFeeRate:{} makerBonusRate:{} minTakerFeeRate:{}",
                header.getOrgId(), header.getUserId(), accountId, clientOrderId, symbolId, builder.getMakerFeeRate().getStr(), builder.getTakerFeeRate().getStr(),
                makerBonusRate.toPlainString(), minTakerFeeRate.toPlainString());

        COIN_ORDER_METRICS.labels("feeRateSetting").observe(System.currentTimeMillis() - timestamp5);
        long timestamp6 = System.currentTimeMillis();
        //设置计划委托行情价格
        BigDecimal quotePrice = spotQuoteService.getCacheCurrentPrice(symbol.getSymbolId(), symbol.getExchangeId(), header.getOrgId());
        builder.setQuotePrice(DecimalUtil.fromBigDecimal(quotePrice));
        NewPlanSpotOrderRequest spotOrderRequest = builder.build();
        BaseResult<NewPlanSpotOrderReply> baseResult = grpcOrderService.createPlanSpotOrder(spotOrderRequest);
        if (!baseResult.isSuccess()) {
            BrokerErrorCode brokerErrorCode = BrokerErrorCode.fromCode(baseResult.getCode());
            return BaseResult.fail(brokerErrorCode != null ? brokerErrorCode : BrokerErrorCode.SYSTEM_ERROR);
        }
        BrokerServerConstants.SUCCESS_CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.SPOT_ORDER_TYPE, header.getPlatform().name()).inc();
        NewPlanSpotOrderReply reply = baseResult.getData();
        BaseResult<CreatePlanSpotOrderResponse> orderResult = BaseResult.success(CreatePlanSpotOrderResponse.newBuilder()
                .setPlanOrder(toBrokerPlanSpotOrder(reply.getOrder(), symbol))
                .build());
        COIN_ORDER_METRICS.labels("orderRequest").observe(System.currentTimeMillis() - timestamp6);
        COIN_ORDER_METRICS.labels("allProcess").observe(System.currentTimeMillis() - startTimestamp);
        //大单或异常单提醒
        pushDataService.userSpotPlanOrderNotifyMessage(header.getOrgId(), accountType, orderResult.getData().getPlanOrder());
        return orderResult;
    }

    private BigDecimal queryTokenIdTotalQuantity(Header header, Long accountId, String tokenId) {
        List<io.bhex.broker.grpc.account.Balance> balanceList = accountService.queryBalance(header, accountId, Collections.singletonList(tokenId));
        return new BigDecimal(balanceList.stream().findFirst().map(io.bhex.broker.grpc.account.Balance::getTotal).orElse("0"));
    }

    public BaseResult<CancelPlanSpotOrderResponse> cancelPlanSpotOrder(Header header, AccountTypeEnum accountType, Integer accountIndex, Long orderId, String clientOrderId) {
        long accountId = getUserAccountId(header, accountIndex, accountType);
        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        CancelPlanSpotOrderRequest.Builder builder = CancelPlanSpotOrderRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountId)
                .setOrderId(orderId)
                .setClientOrderId(clientOrderId)
                .setSignTime(header.getRequestTime());
        io.bhex.base.rc.CancelOrderRequest orderSign = io.bhex.base.rc.CancelOrderRequest.newBuilder()
                .setAccountId(accountId)
                .setClientOrderId(Strings.nullToEmpty(clientOrderId))
                .setOrderId(orderId)
                .setSignTime(signTime)
                .setSignNonce(signNonce)
                .build();
        String sign = "";
        try {
            sign = signUtils.sign(orderSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            log.error("cancel plan spot order add sign occurred a error", e);
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FAILED);
        }
        builder.setSignTime(signTime)
                .setSignNonce(signNonce)
                .setSignBorker(sign);
        CancelPlanSpotOrderRequest request = builder.build();
        BaseResult<CancelPlanSpotOrderReply> baseResult = grpcOrderService.cancelPlanSpotOrder(request);
        if (!baseResult.isSuccess()) {
            if (!baseResult.isSuccess()) {
                BrokerErrorCode brokerErrorCode = BrokerErrorCode.fromCode(baseResult.getCode());
                return BaseResult.fail(brokerErrorCode != null ? brokerErrorCode : BrokerErrorCode.SYSTEM_ERROR);
            }
        }
        CancelPlanSpotOrderReply reply = baseResult.getData();
        io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), reply.getOrder().getSymbolId());
        return BaseResult.success(CancelPlanSpotOrderResponse.newBuilder()
                .setPlanOrder(toBrokerPlanSpotOrder(reply.getOrder(), symbol))
                .build());
    }

    public BatchCancelPlanSpotOrderResponse batchCancelPlanSpotOrder(Header header, AccountTypeEnum accountType, int accountIndex,
                                                                     String symbolId, OrderSide orderSide) {
        long accountId = getUserAccountId(header, accountIndex, accountType);
        BatchCancelPlanSpotOrderRequest.Builder builder = BatchCancelPlanSpotOrderRequest.newBuilder()
                .setAccountId(accountId)
                .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                .setSymbolId(symbolId);
        if (orderSide != null && orderSide != OrderSide.UNKNOWN_ORDER_SIDE) {
            builder.addSide(io.bhex.base.proto.OrderSideEnum.valueOf(orderSide.name()));
        }
        // todo: add sign
        BatchCancelPlanSpotOrderReply reply = grpcOrderService.batchCancelPlanSpotOrder(builder.build());
        return BatchCancelPlanSpotOrderResponse.newBuilder()
                .setRet(0)
                .setOrderCount(reply.getOrderCount())
                .build();
    }

    public GetPlanSpotOrderResponse getPlanSpotOrder(GetPlanSpotOrderRequest request) {
        long accountId = getUserAccountId(request.getHeader(), request.getAccountIndex(), request.getAccountType());
        return getGetPlanSpotOrderResponse(request, accountId);
    }

    public QueryPlanSpotOrdersResponse queryPlanSpotOrders(QueryPlanSpotOrdersRequest request) {
        long accountId = getUserAccountId(request.getHeader(), request.getAccountIndex(), request.getAccountType());
        return getQueryPlanSpotOrdersResponse(request, accountId);
    }

    private QueryPlanSpotOrdersResponse getQueryPlanSpotOrdersResponse(QueryPlanSpotOrdersRequest request, long accountId) {
        Header header = request.getHeader();
        String symbolId = request.getSymbolId();
        if (StringUtils.isNoneBlank(symbolId)) {
            io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), symbolId);
            if (symbol != null) {
                symbolId = symbol.getSymbolId();
            }
        }
        List<PlanSpotOrder.PlanOrderStatusEnum> orderStatusList = request.getOrderStatusList();
        List<io.bhex.base.account.PlanSpotOrder.PlanOrderStatusEnum> bhexOrderStatusList = new ArrayList<>();
        orderStatusList.forEach(orderStatus -> {
            if (PlanSpotOrder.PlanOrderStatusEnum.ORDER_REJECTED.equals(orderStatus)) {
                bhexOrderStatusList.add(io.bhex.base.account.PlanSpotOrder.PlanOrderStatusEnum.ORDER_REJECTED);
                bhexOrderStatusList.add(io.bhex.base.account.PlanSpotOrder.PlanOrderStatusEnum.ORDER_FAILED);
            } else {
                bhexOrderStatusList.add(io.bhex.base.account.PlanSpotOrder.PlanOrderStatusEnum.valueOf(orderStatus.name()));
            }
        });
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (request.getStartTime() == 0 || request.getStartTime() < sevenDaysBefore)) {
            request.toBuilder().setStartTime(sevenDaysBefore);
        }
        GetPlanSpotOrdersRequest.Builder builder = GetPlanSpotOrdersRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setAccountId(accountId)
                .setSymbolId(Strings.nullToEmpty(symbolId))
                .setFromOrderId(request.getFromOrderId())
                .setEndOrderId(request.getEndOrderId())
                .setStartTime(request.getStartTime())
                .setEndTime(request.getEndTime())
                .setLimit(request.getLimit())
                .setBaseTokenId(request.getBaseTokenId())
                .setQuoteTokenId(request.getQuoteTokenId())
                .addAllOrderStatus(bhexOrderStatusList);
        OrderSide orderSide = request.getOrderSide();
        if (orderSide != null && orderSide != OrderSide.UNKNOWN_ORDER_SIDE) {
            builder.addSide(io.bhex.base.proto.OrderSideEnum.valueOf(orderSide.name()));
        }
        GetPlanSpotOrdersRequest ordersRequest = builder.build();
        GetPlanSpotOrdersReply reply = grpcOrderService.getPlanSpotOrders(ordersRequest);
        List<PlanSpotOrder> orderList = Lists.newArrayList();
        if (reply.getOrdersList() != null && reply.getOrdersList().size() > 0) {
            reply.getOrdersList()
                    .forEach(order -> {
                        io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(order.getOrgId(), order.getSymbolId());
                        orderList.add(toBrokerPlanSpotOrder(order, symbol));
                    });
        }
        QueryPlanSpotOrdersResponse.Builder responseBuilder = QueryPlanSpotOrdersResponse.newBuilder();
        if (request.getFromOrderId() == 0 && request.getEndOrderId() > 0 && header.getPlatform() != Platform.OPENAPI) {
            responseBuilder.addAllOrders(Lists.reverse(orderList));
        } else {
            responseBuilder.addAllOrders(orderList);
        }
        return responseBuilder.build();
    }

    private long getUserAccountId(Header header, int accountIndex, AccountTypeEnum accountTypeEnum) {
        Long accountId;
        if (accountTypeEnum == AccountTypeEnum.MARGIN) {
            accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        } else {
            accountId = accountService.getMainIndexAccountId(header, accountIndex);
        }
        return accountId;
    }

    public GetPlanSpotOrderResponse adminGetPlanSpotOrder(GetPlanSpotOrderRequest request) {
        return getGetPlanSpotOrderResponse(request, request.getAccountId());
    }

    private GetPlanSpotOrderResponse getGetPlanSpotOrderResponse(GetPlanSpotOrderRequest request, Long accountId) {
        long orgId = request.getHeader().getOrgId();
        io.bhex.base.account.GetPlanSpotOrderRequest spotOrderRequest = io.bhex.base.account.GetPlanSpotOrderRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(accountId)
                .setOrderId(request.getOrderId())
                .setClientOrderId(request.getClientOrderId())
                .build();
        GetPlanSpotOrderReply reply = grpcOrderService.getPlanSpotOrder(spotOrderRequest);
        io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(orgId, reply.getPlanOrder().getSymbolId());
        PlanSpotOrder planSpotOrder = toBrokerPlanSpotOrder(reply.getPlanOrder(), symbol);
        //判断是否添加关联订单信息
        GetPlanSpotOrderResponse.Builder builder = GetPlanSpotOrderResponse.newBuilder()
                .setPlanOrder(planSpotOrder);
        if (planSpotOrder.getExecutedOrderId() > 0) {
            GetOrderRequest getOrderRequest = GetOrderRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setAccountId(accountId)
                    .setOrderId(planSpotOrder.getExecutedOrderId())
                    .build();
            try {
                GetOrderReply getOrderReply = grpcOrderService.getOrderInfo(getOrderRequest);
                builder.setOrder(getOrder(getOrderReply.getOrder()));
            } catch (Exception e) {
                //忽略该异常,确保计划委托订单基本信息可以返回
                log.error("get plan spot order detail order fail!accountId:{},orderId:{},executedId:{}", accountId, planSpotOrder.getOrderId(), planSpotOrder.getExecutedOrderId(), e);
            }
        }
        return builder.build();
    }

    public QueryPlanSpotOrdersResponse adminQueryPlanSpotOrders(QueryPlanSpotOrdersRequest request) {
        return getQueryPlanSpotOrdersResponse(request, request.getAccountId());
    }

    public PlanSpotOrder toBrokerPlanSpotOrder(io.bhex.base.account.PlanSpotOrder order, io.bhex.broker.grpc.basic.Symbol symbol) {
        OrderType orderType;
        switch (order.getOrderType()) {
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
        PlanSpotOrder.Builder builder = PlanSpotOrder.newBuilder()
                .setOrderId(order.getOrderId())
                .setAccountId(order.getAccountId())
                .setClientOrderId(order.getClientOrderId())
                .setOrgId(order.getOrgId())
                .setExchangeId(order.getExchangeId())
                .setSymbolId(order.getSymbolId())
                .setSymbolName(symbol == null ? "" : symbol.getSymbolName())
                .setBaseTokenId(symbol == null ? order.getBaseTokenId() : symbol.getBaseTokenId())
                .setBaseTokenName(symbol == null ? "" : symbol.getBaseTokenName())
                .setQuoteTokenId(symbol == null ? order.getQuoteTokenId() : symbol.getQuoteTokenId())
                .setQuoteTokenName(symbol == null ? "" : symbol.getQuoteTokenName())
                .setTriggerPrice(DecimalUtil.toBigDecimal(order.getTriggerPrice()).stripTrailingZeros().toPlainString())
                .setTriggerTime(order.getTriggerTime())
                .setQuotePrice(DecimalUtil.toBigDecimal(order.getQuotePrice()).stripTrailingZeros().toPlainString())
                .setPrice(DecimalUtil.toBigDecimal(order.getPrice()).stripTrailingZeros().toPlainString())
                .setOrigQty(order.getOrderType() == OrderTypeEnum.MARKET_OF_QUOTE
                        ? DecimalUtil.toBigDecimal(order.getAmount()).stripTrailingZeros().toPlainString()
                        : DecimalUtil.toBigDecimal(order.getQuantity()).stripTrailingZeros().toPlainString())
                .setExecutedPrice(DecimalUtil.toBigDecimal(order.getExecutedPrice()).stripTrailingZeros().toPlainString())
                .setExecutedQuantity(DecimalUtil.toBigDecimal(order.getExecutedQuantity()).stripTrailingZeros().toPlainString())
                .setStatus(toPlanSpotOrderStatus(order))
                .setTime(order.getCreatedTime())
                .setLastUpdated(order.getUpdatedTime())
                .setOrderType(orderType)
                .setExecutedOrderId(order.getExecutedOrderId())
                .setSide(OrderSide.valueOf(order.getSide().name()));
        return builder.build();
    }

    private PlanSpotOrder.PlanOrderStatusEnum toPlanSpotOrderStatus(io.bhex.base.account.PlanSpotOrder order) {
        if (order.getStatus() == io.bhex.base.account.PlanSpotOrder.PlanOrderStatusEnum.ORDER_FILLED) {
            /*
             * 平台里计划委托单判断委托是否成功是根据status和triggerStatus联合判断
             * 当status=ORDER_FILLED && triggerStatus == SUCCESS 时是委托成功，否则是委托失败
             */
            if (order.getTriggerStatus() == io.bhex.base.account.PlanSpotOrder.PlanOrderTriggerStatusEnum.SUCCESS) {
                return PlanSpotOrder.PlanOrderStatusEnum.ORDER_FILLED;
            } else {
                return PlanSpotOrder.PlanOrderStatusEnum.ORDER_REJECTED;
            }
        } else if (order.getStatus() == io.bhex.base.account.PlanSpotOrder.PlanOrderStatusEnum.ORDER_FAILED) {
            return PlanSpotOrder.PlanOrderStatusEnum.ORDER_REJECTED;
        } else {
            return PlanSpotOrder.PlanOrderStatusEnum.valueOf(order.getStatus().name());
        }
    }
}
