/*
 ************************************
 * @项目名称: broker-server
 * @文件名称: OptionOrderService
 * @Date 2019/01/09
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.protobuf.TextFormat;

import io.bhex.broker.grpc.user.level.QueryMyLevelConfigResponse;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.util.GrpcRequestUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.account.BatchCancelOrderReply;
import io.bhex.base.account.BatchCancelOrderRequest;
import io.bhex.base.account.CancelOrderReply;
import io.bhex.base.account.CancelOrderRequest;
import io.bhex.base.account.GetOptionSettlementListReply;
import io.bhex.base.account.GetOptionSettlementListReq;
import io.bhex.base.account.GetOrderReply;
import io.bhex.base.account.GetOrderRequest;
import io.bhex.base.account.GetOrderTradeDetailReply;
import io.bhex.base.account.GetOrderTradeDetailRequest;
import io.bhex.base.account.GetOrdersReply;
import io.bhex.base.account.GetOrdersRequest;
import io.bhex.base.account.GetSettlementStatusReply;
import io.bhex.base.account.GetSettlementStatusReq;
import io.bhex.base.account.GetTradesReply;
import io.bhex.base.account.GetTradesRequest;
import io.bhex.base.account.NewOrderReply;
import io.bhex.base.account.NewOrderRequest;
import io.bhex.base.account.OptionPositions;
import io.bhex.base.account.OptionPositionsReply;
import io.bhex.base.account.OptionPositionsReq;
import io.bhex.base.account.OptionSettlementReply;
import io.bhex.base.account.OptionSettlementReq;
import io.bhex.base.account.OptionSettlements;
import io.bhex.base.account.Trade;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.proto.OrderStatusEnum;
import io.bhex.base.proto.OrderTimeInForceEnum;
import io.bhex.base.proto.OrderTypeEnum;
import io.bhex.base.rc.OrderSign;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenDetail;
import io.bhex.base.token.TokenOptionInfo;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteListType;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.order.BatchCancelOrderResponse;
import io.bhex.broker.grpc.order.CancelOrderResponse;
import io.bhex.broker.grpc.order.CreateOrderResponse;
import io.bhex.broker.grpc.order.Fee;
import io.bhex.broker.grpc.order.GetBanSellConfigResponse;
import io.bhex.broker.grpc.order.GetOptionSettleStatusResponse;
import io.bhex.broker.grpc.order.GetOrderFeeRateResponse;
import io.bhex.broker.grpc.order.GetOrderMatchResponse;
import io.bhex.broker.grpc.order.GetOrderResponse;
import io.bhex.broker.grpc.order.HistoryOptionRequest;
import io.bhex.broker.grpc.order.HistoryOptionsResponse;
import io.bhex.broker.grpc.order.MatchInfo;
import io.bhex.broker.grpc.order.OptionPosition;
import io.bhex.broker.grpc.order.OptionPositionsResponse;
import io.bhex.broker.grpc.order.OptionSettleDetail;
import io.bhex.broker.grpc.order.OptionSettleStatus;
import io.bhex.broker.grpc.order.OptionSettlement;
import io.bhex.broker.grpc.order.OptionSettlementResponse;
import io.bhex.broker.grpc.order.Order;
import io.bhex.broker.grpc.order.OrderFeeRate;
import io.bhex.broker.grpc.order.OrderQueryType;
import io.bhex.broker.grpc.order.OrderSide;
import io.bhex.broker.grpc.order.OrderStatus;
import io.bhex.broker.grpc.order.OrderType;
import io.bhex.broker.grpc.order.QueryMatchResponse;
import io.bhex.broker.grpc.order.QueryOrdersResponse;
import io.bhex.broker.grpc.order.SettleStatusInfo;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import io.bhex.broker.server.elasticsearch.service.ITradeDetailHistoryService;
import io.bhex.broker.server.grpc.client.service.GrpcOptionOrderService;
import io.bhex.broker.server.grpc.server.service.aspect.SiteFunctionLimitSwitchAnnotation;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.TokenOption;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.TokenOptionMapper;
import io.bhex.broker.server.util.BigDecimalUtil;
import io.bhex.broker.server.util.SignUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class OptionOrderService {

    @Resource
    GrpcOptionOrderService grpcOptionOrderService;

    @Resource
    AccountService accountService;

    @Autowired
    BasicService basicService;

    @Resource
    OptionPriceService optionPriceService;

    private static final BigDecimal MAX_PRICE_VALUE = new BigDecimal(99999);
    private static final BigDecimal MAX_NUMBER = new BigDecimal(99999999);

    private static final Set<Long> optionMarketAccountList = Sets.newHashSet(308516173654374912L, 326667582182529792L, 308507468720355840L);

    @Resource
    SignUtils signUtils;

    @Resource
    SymbolMapper symbolMapper;

    @Resource
    TokenOptionMapper tokenOptionMapper;

    @Resource
    private UserBlackWhiteListConfigService userBlackWhiteListConfigService;

    @Resource
    private UserLevelService userLevelService;

    @Resource
    private ITradeDetailHistoryService tradeDetailHistoryService;

    private Cache<String, io.bhex.base.account.OptionSettlement> optSettlementCache = CacheBuilder
            .newBuilder()
            .maximumSize(6000)
            .expireAfterWrite(2, TimeUnit.HOURS)
            .build();

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_OPTION_TRADE_KEY, userSwitchGroupKey = BaseConfigConstants.FROZEN_USER_OPTION_TRADE_GROUP)
    public CreateOrderResponse newOptionOrder(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex,
                                              Long exchangeId, String symbolId, String clientOrderId,
                                              OrderType orderType, OrderSide orderSide, String price, String qty,
                                              io.bhex.broker.grpc.order.OrderTimeInForceEnum timeInForceEnum, String orderSource) {
        BrokerServerConstants.CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.OPTION_ORDER_TYPE, header.getPlatform().name()).inc();
        //判断期权是否交割
        TokenOptionInfo tokenOptionInfo = basicService.getTokenOptionInfo(symbolId);
        if (tokenOptionInfo == null) {
            log.warn("create order cannot find option token.");
            throw new BrokerException(BrokerErrorCode.OPTION_NOT_EXIST);
        }

        if (tokenOptionInfo.getSettlementDate() < new Date().getTime()) {
            log.warn("create order filed The option has expired.");
            throw new BrokerException(BrokerErrorCode.OPTION_HAS_EXPIRED);
        }

        Long beginTimestamp = System.currentTimeMillis();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        if (orderType == null || orderSide == null
                || orderType == OrderType.UNKNOWN_ORDER_TYPE || orderSide == OrderSide.UNKNOWN_ORDER_SIDE) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        NewOrderRequest.Builder builder = NewOrderRequest.newBuilder();
        OrderSideEnum orderSideEnum = OrderSideEnum.valueOf(orderSide.name());

        //校验是否禁售,该用户是否在白名单内
        if (orderSide.getNumber() == OrderSideEnum.SELL.getNumber()) {
            GetBanSellConfigResponse isOk
                    = getBanSellConfig(header.getUserId(), header.getOrgId(), exchangeId, symbolId);
            if (isOk == null || !isOk.getCanSell()) {
                throw new BrokerException(BrokerErrorCode.ORDER_HAS_BEEN_FILLED);
            }
        }

        BigDecimal quantity = new BigDecimal(qty).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        SymbolDetail symbolDetail = basicService.getSymbolDetailOption(exchangeId, symbolId);

        if (symbolDetail == null) {
            log.warn("create order cannot find symbol detail. order:{}", TextFormat.shortDebugString(builder.build()));
            throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        // 校验限价单的下单价格
        if (orderType != OrderType.MARKET) {
            // 价格校验   最大值、最小值、小数位数
            validPrice(new BigDecimal(price), symbolDetail);
        }
        // 校验限价单的下单数量或者市价卖单的下单数量
        if (orderType != OrderType.MARKET || orderSide == OrderSide.SELL) {
            // 数量校验   最大值、最小值
            validQuantity(quantity, symbolDetail);

        }

        // 校验最小交易额
        BigDecimal minTradeAmount = DecimalUtil.toBigDecimal(symbolDetail.getMinTradeAmount());
        validTradeAmount(exchangeId, symbolId, orderType, price, builder, quantity, minTradeAmount, header.getOrgId());

        builder.setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setExchangeId(exchangeId)
                .setSymbolId(symbolId)
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
            case MARKET:
                builder.setOrderType(OrderTypeEnum.MARKET_OF_BASE);
                builder.setQuantity(DecimalUtil.fromBigDecimal(quantity));
                break;
            default:
                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }


        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        builder.setSignTime(signTime).setSignNonce(signNonce);

        Long timestampFlag1 = System.currentTimeMillis();
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

        builder.setOrderSource(basicService.getOrderSource(header.getOrgId(), header.getPlatform(), orderSource));
        //校验精度位数
        BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(price) ? price : "", quantity != null ? quantity.toPlainString() : "");
        try {
            long startTime = System.currentTimeMillis();
            if (!optionMarketAccountList.contains(accountId)) {
                QueryMyLevelConfigResponse myVipLevelConfig = userLevelService.queryMyLevelConfig(header.getOrgId(), header.getUserId(), false, false);
                if (myVipLevelConfig != null) {
                    if (orderSide == OrderSide.BUY) {
                        BigDecimal buyMakerDiscount = new BigDecimal(myVipLevelConfig.getOptionBuyMakerDiscount());
                        BigDecimal buyTakerDiscount = new BigDecimal(myVipLevelConfig.getOptionBuyTakerDiscount());
                        builder.setMakerFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal("0.005").multiply(buyMakerDiscount).setScale(8, RoundingMode.UP)));
                        builder.setTakerFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal("0.005").multiply(buyTakerDiscount).setScale(8, RoundingMode.UP)));
                    } else {
                        BigDecimal sellMakerDiscount = new BigDecimal(myVipLevelConfig.getOptionSellMakerDiscount());
                        BigDecimal sellTakerDiscount = new BigDecimal(myVipLevelConfig.getOptionSellTakerDiscount());
                        builder.setMakerFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal("0.005").multiply(sellMakerDiscount).setScale(8, RoundingMode.UP)));
                        builder.setTakerFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal("0.005").multiply(sellTakerDiscount).setScale(8, RoundingMode.UP)));
                    }
                } else {
                    builder.setMakerFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal("0.005")));
                    builder.setTakerFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal("0.005")));
                }
            } else {
                builder.setMakerFeeRate(DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
                builder.setTakerFeeRate(DecimalUtil.fromBigDecimal(BigDecimal.ZERO));
            }
            long endTime = System.currentTimeMillis();
            log.info("handel broker fee use time {} clientOrderId {} ", (endTime - startTime), clientOrderId);
        } catch (Exception ex) {
            log.error("handel broker fee config fail userId {} accountId {} symbolId {} clientOrderId {} error {}", header.getUserId(), accountId, symbolId, clientOrderId, ex);
        }

        Long timestampFlag2 = System.currentTimeMillis();
        NewOrderReply reply = grpcOptionOrderService.createOptionOrder(builder.build());
        BrokerServerConstants.SUCCESS_CREATE_ORDER_COUNTER.labels(String.valueOf(header.getOrgId()), BrokerServerConstants.OPTION_ORDER_TYPE, header.getPlatform().name()).inc();
        Long endTimestamp = System.currentTimeMillis();
        log.debug("createOrder time-consuming: orderId:{}, builderRequest: {}, buildSign: {}, doGrpc: {}", reply.getOrder().getOrderId(),
                timestampFlag1 - beginTimestamp, timestampFlag2 - timestampFlag1, endTimestamp - timestampFlag2);

        return CreateOrderResponse.newBuilder().setOrder(getOptionOrder(reply.getOrder(), symbolDetail, exchangeId)).build();
    }

    private void validTradeAmount(Long exchangeId, String symbolId, OrderType orderType, String price,
                                  NewOrderRequest.Builder builder, BigDecimal quantity, BigDecimal minTradeAmount, Long orgId) {
        BigDecimal amount = BigDecimal.ZERO;
        if (orderType == OrderType.LIMIT || orderType == OrderType.LIMIT_MAKER) {
            amount = new BigDecimal(price).multiply(quantity).multiply(BigDecimal.valueOf(1.1D));
        } else if (orderType == OrderType.MARKET) {
            BigDecimal currentPrice = optionPriceService.getCurrentOptionPrice(symbolId, exchangeId, orgId);
            if (currentPrice != null && currentPrice.compareTo(BigDecimal.ZERO) > 0) {
                amount = currentPrice.multiply(quantity).multiply(BigDecimal.valueOf(1.1D));
            }
        }

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

    private void validPrice(BigDecimal price, SymbolDetail symbol) {
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

    public GetOrderResponse getOptionOrderInfo(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex, Long orderId, String clientOrderId) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        GetOrderRequest request = GetOrderRequest.newBuilder()
                .setAccountId(accountId)
                .setOrderId(orderId)
                .setClientOrderId(clientOrderId)
                .build();
        GetOrderReply reply = grpcOptionOrderService.getOptionOrderInfo(request);

        ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
        Symbol symbol = optionSymbolMap.get(header.getOrgId() + "_" + reply.getOrder().getSymbolId());
        SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
        return GetOrderResponse.newBuilder().setOrder(getOptionOrder(reply.getOrder(), symbolDetail, symbol.getExchangeId())).build();
    }

    public CancelOrderResponse cancelOptionOrder(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex, Long orderId, String clientOrderId) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();

        CancelOrderRequest.Builder builder = CancelOrderRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountId)
                .setOrderId(orderId)
                .setClientOrderId(clientOrderId);

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
        builder.setSignTime(signTime)
                .setSignNonce(signNonce)
                .setSignBorker(sign);

        CancelOrderReply reply = grpcOptionOrderService.cancelOptionOrder(builder.build());

        SymbolDetail symbolDetail = null;
        String key = String.format("%s_%s", header.getOrgId(), reply.getOrder().getSymbolId());
        Symbol symbol = basicService.getTokenSymbolOptionMap().get(key);

        Long exchangeId = 0L;
        if (symbol != null) {
            symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
            exchangeId = symbol.getExchangeId();
        }
        return CancelOrderResponse.newBuilder().setOrder(getOptionOrder(reply.getOrder(), symbolDetail, exchangeId)).build();
    }

    public BatchCancelOrderResponse batchCancelOptionOrder(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex,
                                                           List<String> symbolIds, OrderSide orderSide) {
        BatchCancelOrderRequest.Builder builder = BatchCancelOrderRequest.newBuilder();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        builder.setAccountId(accountId);
        if (symbolIds != null && symbolIds.size() > 0) {
            builder.addAllSymbolIds(symbolIds);
        }
        if (orderSide != null && orderSide != OrderSide.UNKNOWN_ORDER_SIDE) {
            builder.addSides(OrderSideEnum.valueOf(orderSide.name()));
        }
        // todo: add sign
        BatchCancelOrderReply reply = grpcOptionOrderService.batchCancelOptionOrder(builder.build());
        return BatchCancelOrderResponse.getDefaultInstance();
    }

    public QueryOrdersResponse queryOptionOrders(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex,
                                                 String symbolId, Long fromOrderId, Long endOrderId, Long startTime, Long endTime,
                                                 String baseTokenId, String quoteTokenId, OrderType orderType, OrderSide orderSide,
                                                 Integer limit, OrderQueryType orderQueryType, List<OrderStatus> orderStatus) {
        if (!BrokerService.checkModule(header, FunctionModule.OPTION)) {
            return QueryOrdersResponse.getDefaultInstance();
        }
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        List<Order> orderList = queryOptionOrders(header, accountId,
                symbolId, fromOrderId, endOrderId, startTime, endTime,
                baseTokenId, quoteTokenId, orderType, orderSide,
                limit, orderQueryType, orderStatus);
        return QueryOrdersResponse.newBuilder().addAllOrders(orderList).build();
    }

    private List<Order> queryOptionOrders(Header header, Long accountId,
                                          String symbolId, Long fromOrderId, Long endOrderId, Long startTime, Long endTime,
                                          String baseTokenId, String quoteTokenId, OrderType orderType, OrderSide orderSide,
                                          Integer limit, OrderQueryType queryType, List<OrderStatus> orderStatus) {
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
                if (orderStatus.size() > 1) {
                    orderStatusList = Lists.newArrayList(OrderStatusEnum.CANCELED, OrderStatusEnum.FILLED);
                } else {
                    OrderStatus status = orderStatus.get(0);
                    if (OrderStatus.CANCELED.name().equals(status.name())) {
                        orderStatusList = Lists.newArrayList(OrderStatusEnum.CANCELED);
                    } else if (OrderStatus.FILLED.name().equals(status.name())) {
                        orderStatusList = Lists.newArrayList(OrderStatusEnum.FILLED);
                    }
                }
                break;
            default:
                break;
        }

        GetOrdersRequest.Builder builder = GetOrdersRequest.newBuilder()
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
        GetOrdersReply reply = grpcOptionOrderService.queryOptionOrders(request);
        List<Order> orderList = Lists.newArrayList();
        if (reply.getOrdersList() != null && reply.getOrdersList().size() > 0) {
            ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
            reply.getOrdersList().forEach(order -> {
                Symbol symbol = optionSymbolMap.get(header.getOrgId() + "_" + order.getSymbolId());
                if (symbol != null) {
                    SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                    orderList.add(getOptionOrder(order, symbolDetail, symbol.getExchangeId()));
                }
            });
        }
        if (fromOrderId == 0 && endOrderId > 0 && header.getPlatform() != Platform.OPENAPI) {
            return Lists.reverse(orderList);
        }
        return orderList;
    }

    private Order getOptionOrder(io.bhex.base.account.Order order, SymbolDetail symbolDetail, Long exchangeId) {
        String statusCode = order.getStatus().name();
        List<Fee> fees = new ArrayList<>();
        //判断是做多还是做空
        if (order.getSide().name().equals(OrderSide.BUY.name())) {
            fees = order.getTradeFeesList().stream()
                    .map(fee -> Fee.newBuilder()
                            .setFeeTokenId(fee.getToken().getTokenId())
                            .setFeeTokenName(order.getSymbol().getQuoteToken().getTokenName())
                            .setFee(DecimalUtil.toBigDecimal(fee.getFee()).stripTrailingZeros().toPlainString())
                            .build())
                    .collect(Collectors.toList());
        } else {
            fees = order.getTradeFeesList().stream()
                    .map(fee -> Fee.newBuilder()
                            .setFeeTokenId(fee.getToken().getTokenId())
                            .setFeeTokenName(fee.getToken().getTokenName())
                            .setFee(DecimalUtil.toBigDecimal(fee.getFee()).stripTrailingZeros().toPlainString())
                            .build())
                    .collect(Collectors.toList());
        }

        OrderType orderType;
        switch (order.getType()) {
            case LIMIT:
                orderType = OrderType.LIMIT;
                break;
            default:
                orderType = OrderType.MARKET;
                break;
        }

        String price;
        String origQty;
        String executedQty;
        String executedAmount;
        String avgPrice;
        if (symbolDetail != null) {
            int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
            int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
            int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();
            price = DecimalUtil.toBigDecimal(order.getPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
            origQty = order.getType() == OrderTypeEnum.MARKET_OF_QUOTE
                    ? DecimalUtil.toBigDecimal(order.getAmount()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString()
                    : DecimalUtil.toBigDecimal(order.getQuantity()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
            executedQty = DecimalUtil.toBigDecimal(order.getExecutedQuantity()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
            executedAmount = DecimalUtil.toBigDecimal(order.getExecutedAmount()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
            avgPrice = DecimalUtil.toBigDecimal(order.getAveragePrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
        } else {
            price = DecimalUtil.toTrimString(order.getPrice());
            origQty = DecimalUtil.toTrimString(order.getType() == OrderTypeEnum.MARKET_OF_QUOTE ? order.getAmount() : order.getQuantity());
            executedQty = DecimalUtil.toTrimString(order.getExecutedQuantity());
            executedAmount = DecimalUtil.toTrimString(order.getExecutedAmount());
            avgPrice = DecimalUtil.toTrimString(order.getAveragePrice());
        }

        return Order.newBuilder()
                .setExchangeId(exchangeId)
                .setAccountId(order.getAccountId())
                .setOrderId(order.getOrderId())
                .setClientOrderId(order.getClientOrderId())
                .setSymbolId(order.getSymbol().getSymbolId())
                .setSymbolName(order.getSymbol().getSymbolName())
                .setBaseTokenId(order.getSymbol().getBaseToken().getTokenId())
                .setBaseTokenName(order.getSymbol().getBaseToken().getTokenName())
                .setQuoteTokenId(order.getSymbol().getQuoteToken().getTokenId())
                .setQuoteTokenName(order.getSymbol().getQuoteToken().getTokenName())
                .setPrice(price)
                .setOrigQty(origQty)
                .setExecutedQty(executedQty)
                .setExecutedAmount(executedAmount)
                .setAvgPrice(avgPrice)
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(order.getSide().name()))
                .addAllFees(fees)
                .setStatusCode(statusCode)
                .setTime(order.getCreatedTime())
                .setTimeInForce(io.bhex.broker.grpc.order.OrderTimeInForceEnum.valueOf(order.getTimeInForce().name()))
                .build();
    }

    public QueryMatchResponse queryOptionMatchInfo(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex, String symbolId, Long fromTraderId, Long endTradeId,
                                                   Long startTime, Long endTime, Integer limit, OrderSide orderSide, boolean fromEsHistory) {
        if (!BrokerService.checkModule(header, FunctionModule.OPTION)) {
            return QueryMatchResponse.getDefaultInstance();
        }
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        GetTradesRequest.Builder builder = GetTradesRequest.newBuilder()
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
        ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
        List<MatchInfo> matchInfoList = Lists.newArrayList();

        if (fromTraderId == 0 && endTradeId > 0) {
            // 先查询ES
            List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, 0L, symbolId, "", orderSideValue, startTime, endTime, fromTraderId, endTradeId, limit, false);
            matchInfoList.addAll(convertEsTradeDetailList(header, optionSymbolMap, tradeDetailList));
            // 再查询shard
            if (tradeDetailList.size() < limit) {
                Long nextId = endTradeId;
                if (tradeDetailList.size() > 0) {
                    nextId = tradeDetailList.get(tradeDetailList.size() - 1).getTradeDetailId();
                }
                builder.setEndTradeId(nextId).setLimit(limit - tradeDetailList.size());
                GetTradesReply reply = grpcOptionOrderService.queryOptionMatchInfo(builder.build());
                matchInfoList.addAll(convertBhTradesList(header, optionSymbolMap, reply.getTradesList()));
            }
        } else {
            // 先查询shard
            GetTradesReply reply = grpcOptionOrderService.queryOptionMatchInfo(builder.build());
            matchInfoList.addAll(convertBhTradesList(header, optionSymbolMap, reply.getTradesList()));
            // 再查询ES
            if (reply.getTradesCount() < limit) {
                Long nextId = fromTraderId;
                if (reply.getTradesCount() > 0) {
                    nextId = reply.getTradesList().get(reply.getTradesCount() - 1).getTradeId();
                }
                List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, 0L, symbolId, "", orderSideValue, startTime, endTime,
                        nextId, endTradeId, limit - reply.getTradesCount(), false);
                matchInfoList.addAll(convertEsTradeDetailList(header, optionSymbolMap, tradeDetailList));
            }
        }
        if (fromTraderId == 0 && endTradeId > 0 && header.getPlatform() != Platform.OPENAPI) {
            return QueryMatchResponse.newBuilder().addAllMatch(Lists.reverse(matchInfoList)).build();
        }
        return QueryMatchResponse.newBuilder().addAllMatch(matchInfoList).build();
    }

    public GetOrderMatchResponse getOptionOrderMatchInfo(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex, Long orderId, Long fromTraderId, Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.OPTION)) {
            return GetOrderMatchResponse.getDefaultInstance();
        }
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        if (orderId <= 0) {
            return GetOrderMatchResponse.getDefaultInstance();
        }
        List<MatchInfo> matchInfoList = Lists.newArrayList();

        ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
        List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, 0, orderId, "", "", null,
                0, 0, 0L, 0L, limit, false);
        if (!CollectionUtils.isEmpty(tradeDetailList)) {
            matchInfoList.addAll(convertEsTradeDetailList(header, optionSymbolMap, tradeDetailList));
        } else {
            GetOrderTradeDetailRequest request = GetOrderTradeDetailRequest.newBuilder()
                    .setAccountId(this.accountService.getOptionAccountId(header))
                    .setOrderId(orderId)
                    .setFromTradeId(fromTraderId)
                    .setLimit(limit)
                    .build();
            GetOrderTradeDetailReply reply = grpcOptionOrderService.getOptionMatchInfo(request);
            matchInfoList.addAll(convertBhTradesList(header, optionSymbolMap, reply.getTradesList()));
        }
        return GetOrderMatchResponse.newBuilder().addAllMatch(matchInfoList).build();
    }

    private List<MatchInfo> convertBhTradesList(Header header, Map<String, Symbol> optionSymbolMap, List<Trade> trades) {
        return trades.stream().map(trade -> {
            Symbol symbol = optionSymbolMap.get(header.getOrgId() + "_" + trade.getSymbolId());
            if (symbol != null) {
                SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                return getMatchInfo(trade, symbolDetail);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private List<MatchInfo> convertEsTradeDetailList(Header header, Map<String, Symbol> optionSymbolMap, List<TradeDetail> trades) {
        return trades.stream().map(trade -> {
            Symbol symbol = optionSymbolMap.get(header.getOrgId() + "_" + trade.getSymbolId());
            if (symbol != null) {
                SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                return getMatchInfo(trade, symbolDetail);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

//    public GetBanSellConfigResponse getBanSellConfig(Long userId, Long orgId, Long exchangeId, String symbolId) {
//        Long accountId = this.accountService.getAccountId(orgId, userId);
//        if (accountId == null) {
//            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
//        }
//        Symbol symbol = symbolMapper.getBySymbolId(exchangeId, symbolId, orgId);
//        if (symbol == null) {
//            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
//        }
//
//        if (symbol.getBanSellStatus() == null) {
//            return GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
//        }
//
//        if (symbol.getBanSellStatus().intValue() == 0) {
//            return GetBanSellConfigResponse.newBuilder().setCanSell(true).build();
//        }
//
//        if (symbol.getBanSellStatus().intValue() == 1) {
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

    public GetBanSellConfigResponse getBanSellConfig(Long userId, Long orgId, Long exchangeId, String symbolId) {

        Symbol symbol = symbolMapper.getBySymbolId(exchangeId, symbolId, orgId);
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

    private MatchInfo getMatchInfo(TradeDetail trade, SymbolDetail symbolDetail) {
        OrderType orderType;
        switch (trade.getOrderType()) {
            case 4:
                orderType = OrderType.LIMIT_MAKER;
                break;
            case 0:
                orderType = OrderType.LIMIT;
                break;
            default:
                orderType = OrderType.MARKET;
                break;
        }

        Fee fee = Fee.newBuilder()
                .setFeeTokenId(trade.getFeeToken())
                .setFeeTokenName(basicService.getTokenName(trade.getOrgId(), trade.getFeeToken()))
                .setFee(trade.getFee().stripTrailingZeros().toPlainString())
                .build();

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();
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
                .setPrice(trade.getPrice().setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setQuantity(trade.getQuantity().setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAmount(trade.getAmount().setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setFee(fee)
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(OrderSideEnum.forNumber(trade.getOrderSide()).name()))
                .setMatchOrgId(trade.getMatchOrgId() != null && trade.getOrgId().equals(trade.getMatchOrgId()) ? trade.getMatchOrgId() : 0)
                .setMatchUserId(trade.getMatchOrgId() != null && trade.getMatchUserId() != null && trade.getOrgId().equals(trade.getMatchOrgId()) ? trade.getMatchUserId() : 0)
                .setMatchAccountId(trade.getMatchAccountId() != null ? trade.getMatchAccountId() : 0)
//                .setTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(trade.getMatchTime(), new ParsePosition(1)).getTime())
                .setTime(trade.getMatchTime().getTime())
                .setIsMaker(trade.getIsMaker() == 1)
                .build();
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
            default:
                orderType = OrderType.MARKET;
                break;
        }

//        Fee fee = null;
//        //判断是做多还是做空
//        if (trade.getSide().name().equals(OrderSide.BUY.name())) {
//            fee = Fee.newBuilder()
//                    .setFeeTokenId(trade.getSymbol().getQuoteToken().getTokenId())
//                    .setFeeTokenName(trade.getSymbol().getQuoteToken().getTokenName())
//                    .setFee(DecimalUtil.toBigDecimal(trade.getTradeFee().getFee()).stripTrailingZeros().toPlainString())
//                    .build();
//        } else {
//            fee = Fee.newBuilder()
//                    .setFeeTokenId(trade.getTradeFee().getToken().getTokenId())
//                    .setFeeTokenName(trade.getTradeFee().getToken().getTokenName())
//                    .setFee(DecimalUtil.toBigDecimal(trade.getTradeFee().getFee()).stripTrailingZeros().toPlainString())
//                    .build();
//        }

        Fee fee = Fee.newBuilder()
                .setFeeTokenId(trade.getTradeFee().getToken().getTokenId())
                .setFeeTokenName(trade.getTradeFee().getToken().getTokenName())
                .setFee(DecimalUtil.toBigDecimal(trade.getTradeFee().getFee()).stripTrailingZeros().toPlainString())
                .build();

        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();
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
                .setPrice(DecimalUtil.toBigDecimal(trade.getPrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setQuantity(DecimalUtil.toBigDecimal(trade.getQuantity()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAmount(DecimalUtil.toBigDecimal(trade.getAmount()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setFee(fee)
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(trade.getSide().name()))
                .setMatchOrgId(trade.getOrgId() == trade.getMatchOrgId() ? trade.getMatchOrgId() : 0)
                .setMatchUserId(trade.getOrgId() == trade.getMatchOrgId() ? Long.parseLong(trade.getMatchBrokerUserId()) : 0)
                .setMatchAccountId(trade.getMatchAccountId())
                .setTime(trade.getMatchTime())
                .setIsMaker(trade.getIsMaker())
                .build();
    }

    /**
     * 获取期权持仓数据
     *
     * @param header   header
     * @param tokenIds tokenIds
     * @return 持仓数据
     */
    public OptionPositionsResponse getOptionPositions(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex,
                                                      String tokenIds, int exchangeId, long fromBalanceId, long endBalanceId, int limit) {
        if (!BrokerService.checkModule(header, FunctionModule.OPTION)) {
            return OptionPositionsResponse.getDefaultInstance();
        }
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        if (accountId == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        OptionPositionsReq positionsReq = OptionPositionsReq
                .newBuilder()
                .setAccountId(accountId)
                .setExchangeId(exchangeId)
                .setFromBalanceId(fromBalanceId)
                .setEndBalanceId(endBalanceId)
                .setLimit(limit)
                .setTokenIds(tokenIds)
                .build();
        OptionPositionsReply optionPositionsReply = grpcOptionOrderService.getOptionPositions(positionsReq);
        List<OptionPosition> optionPositions = new ArrayList<>();
        optionPositionsReply.getOptionPositionList().forEach(s -> {
            ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
            Symbol symbol = optionSymbolMap.get(header.getOrgId() + "_" + s.getTokenId());

            ImmutableMap<String, TokenDetail> tokenDetailMap = basicService.getTokenDetailOptionMap();
            TokenDetail tokenDetail = tokenDetailMap.get(s.getTokenId());
            BigDecimal price
                    = optionPriceService.getCurrentOptionPrice(s.getSymbolId(), symbol != null ? symbol.getExchangeId() : 0L, header.getOrgId());
            if (symbol != null) {
                SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                optionPositions.add(buildOptionPosition(s, price, symbol, tokenDetail.getTokenOptionInfo().getIndexToken(), symbolDetail, header.getOrgId()));
            }
        });
        return OptionPositionsResponse.newBuilder().addAllOptionPosition(optionPositions).build();
    }

    public HistoryOptionsResponse getHistoryOptions(HistoryOptionRequest request) {
        if (!BrokerService.checkModule(request.getHeader(), FunctionModule.OPTION)) {
            return HistoryOptionsResponse.getDefaultInstance();
        }
        Timestamp now = new Timestamp(System.currentTimeMillis());
        long orgId = request.getHeader().getOrgId();

        List<TokenOption> tokenOptions = queryTokenOptions(now, request.getPageId(), request.getPageSize());
        int total = tokenOptionMapper.selectTokenOptionCount(now);
        List<HistoryOptionsResponse.HistoryOption> options =
                Optional.ofNullable(tokenOptions).orElse(new ArrayList<>())
                        .stream()
                        .map(token -> getHistoryOption(orgId, token))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        return HistoryOptionsResponse.newBuilder()
                .setTotal(total)
                .setPageId(request.getPageId())
                .setPageSize(request.getPageSize())
                .addAllHistoryOption(options)
                .build();
    }

    private HistoryOptionsResponse.HistoryOption getHistoryOption(long orgId, TokenOption t) {
        String key = String.format("%s_%s", orgId, t.getTokenId());
        Symbol symbol = basicService.getTokenSymbolOptionMap().get(key);
        if (symbol == null) {
            return null;
        }

        io.bhex.base.account.OptionSettlement opt = getOptionSettlementCache(t.getTokenId());
        if (opt == null) {
            return null;
        }
        String settlementPrice = DecimalUtil.toTrimString(opt.getSettlementPrice());
        String volume = DecimalUtil.toTrimString(opt.getTradingAmount());
        return HistoryOptionsResponse.HistoryOption.newBuilder()
                .setSymbolId(symbol.getSymbolId())
                .setSymbolName(symbol.getSymbolName())
                .setStrikePrice(DecimalUtil.toTrimString(t.getStrikePrice()))
                .setSettlementTime(t.getSettlementDate().getTime())
                .setSettlementPrice(settlementPrice)
                .setBaseTokenId(symbol.getBaseTokenId())
                .setBaseTokenName(symbol.getBaseTokenName())
                .setQuoteTokenId(symbol.getQuoteTokenId())
                .setQuoteTokenName(symbol.getQuoteTokenName())
                .setVolume(volume)
                .build();
    }

    private io.bhex.base.account.OptionSettlement getOptionSettlementCache(String tokenId) {
        try {
            String key = String.format("opt_settlement_cache:%s", tokenId);
            return optSettlementCache.get(key, () -> getOptionSettlement(tokenId));
        } catch (Exception e) {
            return null;
        }
    }

    private io.bhex.base.account.OptionSettlement getOptionSettlement(String tokenId) {
        GetOptionSettlementListReq request = GetOptionSettlementListReq.newBuilder()
                .addAllTokenIds(Collections.singletonList(tokenId))
                .build();
        GetOptionSettlementListReply reply = grpcOptionOrderService.getOptionSettlementList(request);
        if (CollectionUtils.isEmpty(reply.getOptionSettlementList())) {
            return null;
        }
        return reply.getOptionSettlementList().get(0);
    }

    private List<TokenOption> queryTokenOptions(Timestamp settlementDate, int page, int count) {
        page = (page > 0 ? page : 1);
        count = (count > 0 ? count : BrokerServerConstants.ONE_PAGE_MAX_COUNT);
        return tokenOptionMapper.selectTokenOptions(settlementDate, (page - 1) * count, count);
    }

    /**
     * 构造OptionPosition
     *
     * @param optionPositions 参数
     * @return OptionPosition
     */
    private OptionPosition buildOptionPosition(OptionPositions optionPositions, BigDecimal price, Symbol symbol, String indexToken, SymbolDetail symbolDetail, Long orgId) {
        BigDecimal changedRate = BigDecimal.ZERO;
        BigDecimal positionRights = BigDecimal.ZERO;
        if (DecimalUtil.toBigDecimal(optionPositions.getTotal()).compareTo(BigDecimal.ZERO) > 0) {
            changedRate = getBuyChangedRate(optionPositions, price);
            positionRights = price.multiply(DecimalUtil.toBigDecimal(optionPositions.getTotal()));
        } else if (DecimalUtil.toBigDecimal(optionPositions.getTotal()).compareTo(BigDecimal.ZERO) < 0) {
            changedRate = getSellChangedRate(optionPositions, price);
            positionRights = DecimalUtil
                    .toBigDecimal(optionPositions.getMargin())
                    .subtract(price.multiply(DecimalUtil.toBigDecimal(optionPositions.getTotal()).abs()));
        }
        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();

        BigDecimal totalValue = DecimalUtil.toBigDecimal(optionPositions.getTotal()).stripTrailingZeros();
        BigDecimal priceValue = price.stripTrailingZeros();
        BigDecimal averagePriceValue = DecimalUtil.toBigDecimal(optionPositions.getAveragePrice()).stripTrailingZeros();

        BigDecimal changed =
                new BigDecimal(totalValue.toPlainString())
                        .multiply(priceValue.subtract(averagePriceValue));
        return OptionPosition
                .newBuilder()
                .setExchangeId(symbol.getExchangeId())
                .setAccountId(optionPositions.getAccountId())
                .setSettlementTime(optionPositions.getSettlementTime())
                .setStrikePrice(DecimalUtil.toBigDecimal(optionPositions.getStrikePrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setMargin(DecimalUtil.toBigDecimal(optionPositions.getMargin()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAvailable(DecimalUtil.toBigDecimal(optionPositions.getAvailable()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAveragePrice(averagePriceValue.setScale(minPricePrecision, RoundingMode.DOWN).toPlainString())
                .setBalanceId(optionPositions.getBalanceId())
                .setCostPrice(DecimalUtil.toBigDecimal(optionPositions.getCostPrice()).setScale(quotePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setPrice(priceValue.setScale(minPricePrecision, RoundingMode.DOWN).toPlainString())
                .setAvailPosition(DecimalUtil.toBigDecimal(optionPositions.getAvailPosition()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setSymbolName(optionPositions.getSymbolName())
                .setSymbolId(optionPositions.getSymbolId())
                .setChanged(changed.setScale(2, RoundingMode.DOWN).toPlainString()) //持仓盈亏
                .setChangedRate(changedRate.setScale(2, RoundingMode.DOWN).toPlainString()) //持仓涨跌幅 = (现价金额-持仓均价)\持仓均价*100%
                .setTotal(totalValue.setScale(basePrecision, RoundingMode.DOWN).toPlainString())
                .setIndices(optionPriceService.getIndices(indexToken, orgId).stripTrailingZeros().toPlainString())
                .setQuoteTokenId(symbol.getQuoteTokenId())
                .setQuoteTokenName(symbol.getQuoteTokenName())
                .setBaseTokenId(symbol.getBaseTokenId())
                .setBaseTokenName(symbol.getBaseTokenName())
                .setPositionRights(positionRights.setScale(quotePrecision, RoundingMode.DOWN).toPlainString())
                .build();
    }

    /**
     * 获取做多涨跌幅
     *
     * @param optionPositions optionPositions
     * @param price           price
     * @return bigDecimal
     */
    // 做多 持仓涨跌幅 = (现价金额-持仓均价) \ 持仓均价*100%
    private BigDecimal getBuyChangedRate(OptionPositions optionPositions, BigDecimal price) {
        BigDecimal changedRate = BigDecimal.ZERO;
        BigDecimal averagePrice = DecimalUtil.toBigDecimal(optionPositions.getAveragePrice());
        if (averagePrice.compareTo(BigDecimal.ZERO) == 0) {
            changedRate = BigDecimal.ZERO;
        } else {
            changedRate = (price.subtract(DecimalUtil.toBigDecimal(optionPositions.getAveragePrice()))
                    .divide(averagePrice, 8, RoundingMode.DOWN)
                    .multiply(new BigDecimal("100")));
        }
        return changedRate;
    }

    /**
     * 获取做空涨跌幅 1.85 / 315.07
     *
     * @param optionPositions optionPositions
     * @param price           price
     * @return bigDecimal
     */
    // 做空 持仓涨跌幅 = (现价金额-持仓均价) \ (maxPayOff - 持仓均价) *100% ) * -1
    private BigDecimal getSellChangedRate(OptionPositions optionPositions, BigDecimal price) {
        BigDecimal changedRate = BigDecimal.ZERO;
        BigDecimal cPrice = DecimalUtil.toBigDecimal(optionPositions.getMaxPayOff()).subtract(DecimalUtil.toBigDecimal(optionPositions.getAveragePrice()));
        if (cPrice.compareTo(BigDecimal.ZERO) == 0) {
            changedRate = BigDecimal.ZERO;
        } else {
            changedRate = (price.subtract(DecimalUtil.toBigDecimal(optionPositions.getCostPrice())))
                    .divide(cPrice, 8, RoundingMode.DOWN)
                    .multiply(new BigDecimal("100")).multiply(new BigDecimal("-1"));
        }
        return changedRate;
    }

    /**
     * 交割记录
     *
     * @param header           header
     * @param fromSettlementId fromSettlementId
     * @param endSettlementId  endSettlementId
     * @param startTime        startTime
     * @param endTime          endTime
     * @param limit            limit
     * @return OptionSettlementResponse
     */
    public OptionSettlementResponse getOptionSettlement(Header header, AccountTypeEnum accountTypeEnum, Integer accountIndex,
                                                        String side, Long fromSettlementId, Long endSettlementId, Long startTime, Long endTime, Integer limit) {
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                accountService.getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex)
//                : accountService.getOptionAccountId(header);
        Long accountId = accountService.getOptionAccountId(header);
        if (accountId == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        OptionSettlementReq settlementReq = OptionSettlementReq
                .newBuilder()
                .setAccountId(accountId)
                .setSide(side)
                .setFromSettlementId(fromSettlementId)
                .setEndSettlementId(endSettlementId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit)
                .build();
        OptionSettlementReply optionSettlementReply = grpcOptionOrderService.getOptionSettlement(settlementReq);
        List<OptionSettlement> settlements = new ArrayList<>();
        optionSettlementReply.getOptionSettlementList().forEach(s -> {
            ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
            Symbol symbol = optionSymbolMap.get(header.getOrgId() + "_" + s.getTokenId());
            if (symbol != null) {
                SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                settlements.add(buildOptionSettlement(s, symbol, symbolDetail));
            }
        });
        return OptionSettlementResponse.newBuilder().addAllOptionSettlement(settlements).build();
    }

    /**
     * 构建OptionSettlement
     *
     * @param optionSettlements 参数
     * @return OptionSettlement
     */
    private OptionSettlement buildOptionSettlement(OptionSettlements optionSettlements, Symbol symbol, SymbolDetail symbolDetail) {
        int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
        int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        int quotePrecision = DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().scale();
        BigDecimal cost = DecimalUtil.toBigDecimal(optionSettlements.getAvailable()).abs().multiply(DecimalUtil.toBigDecimal(optionSettlements.getAveragePrice()));
        BigDecimal changed = BigDecimal.ZERO;
        if (DecimalUtil.toBigDecimal(optionSettlements.getAvailable()).compareTo(BigDecimal.ZERO) > 0) {
            changed = DecimalUtil.toBigDecimal(optionSettlements.getChanged()).subtract(cost);
        } else {
            changed = DecimalUtil.toBigDecimal(optionSettlements.getChanged()).add(cost);
        }
        return OptionSettlement
                .newBuilder()
                .setAccountId(optionSettlements.getAccountId())
                .setSymbolName(optionSettlements.getSymbolName())
                .setSymbolId(optionSettlements.getTokenId())
                .setAvailable(DecimalUtil.toBigDecimal(optionSettlements.getAvailable()).setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAveragePrice(DecimalUtil.toBigDecimal(optionSettlements.getAveragePrice()).setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setChanged(changed.setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
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
     * Get order fee rate
     *
     * @param header     request header
     * @param exchangeId order exchangeId
     * @param symbolIds  order symbolId arrays
     * @return order fee rate
     */
    public GetOrderFeeRateResponse getOrderFeeRate(Header header, long exchangeId, List<String> symbolIds) {
//        io.bhex.base.account.GetOrderFeeRateReq req = io.bhex.base.account.GetOrderFeeRateReq.newBuilder()
//                .setAccountId(this.accountService.getOptionAccountId(header))
//                .setExchangeId(exchangeId)
//                .setOrgId(header.getOrgId())
//                .addAllSymbolIds(symbolIds)
//                .build();
//        io.bhex.base.account.GetOrderFeeRateReply reply = grpcOptionOrderService.getOrderFeeRate(req);
//        List<OrderFeeRate> rates = Optional.ofNullable(reply.getOrderFeeRatesList()).orElse(new ArrayList<>())
//                .stream()
//                .map(t -> OrderFeeRate.newBuilder()
//                        .setSymbolId(t.getSymbolId())
//                        .setSymbolName(t.getSymbolName())
//                        .setTakerFeeRate(DecimalUtil.toTrimString(t.getTakerFeeRate()))
//                        .setMakerFeeRate(DecimalUtil.toTrimString(t.getMakerFeeRate()))
//                        .build())
//                .collect(Collectors.toList());
        List<OrderFeeRate> orderFeeRateList = new ArrayList<>();
        symbolIds.forEach(s -> {
            orderFeeRateList.add(OrderFeeRate.newBuilder()
                    .setSymbolId(s)
                    .setSymbolName(s)
                    .setTakerFeeRate(new BigDecimal("0.005").toPlainString())
                    .setMakerFeeRate(new BigDecimal("0.005").toPlainString())
                    .build());
        });
        return GetOrderFeeRateResponse.newBuilder()
                .addAllOrderFeeRates(orderFeeRateList)
                .build();
    }

    public GetOptionSettleStatusResponse getOptionSettleStatus(List<String> symbolIds) {
        if (CollectionUtils.isEmpty(symbolIds)) {
            return GetOptionSettleStatusResponse.newBuilder().build();
        }
        GetSettlementStatusReq req = GetSettlementStatusReq.newBuilder()
                .addAllTokenIds(symbolIds)
                .build();

        List<SettleStatusInfo> list = null;
        try {
            GetSettlementStatusReply reply = grpcOptionOrderService.getSettlementStatus(req);
            list = reply.getStatusInfoList()
                    .stream()
                    .map(t -> {
                        OptionSettleStatus status = OptionSettleStatus.forNumber(t.getSettlementStatus().getNumber());
                        return SettleStatusInfo.newBuilder()
                                .setOptionSettleDetail(toOptionSettleDetail(t.getOptionSettlement()))
                                .setOptionSettleStatus(status)
                                .setSymbolId(t.getSymbolId())
                                .build();
                    }).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("getOptionSettleStatus error", e);
        }

        return GetOptionSettleStatusResponse.newBuilder()
                .addAllSettleStatusInfo(Optional.ofNullable(list).orElse(new ArrayList<>()))
                .build();
    }

    private OptionSettleDetail toOptionSettleDetail(io.bhex.base.account.OptionSettlement t) {
        return OptionSettleDetail.newBuilder()
                .setId(t.getId())
                .setTokenId(t.getTokenId())
                .setSettlementDate(t.getSettlementDate())
                .setSettlementPrice(DecimalUtil.toTrimString(t.getSettlementPrice()))
                .setTradingAmount(DecimalUtil.toTrimString(t.getTradingAmount()))
                .setStatus(t.getStatus())
                .setErrorReason(t.getErrorReason())
                .setCreatedAt(t.getCreatedAt())
                .setUpdatedAt(t.getUpdatedAt())
                .build();
    }
}