package io.bhex.broker.server.grpc.server.service;


import io.bhex.base.margin.cross.*;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.margin.RiskConfig;
import io.bhex.broker.grpc.order.*;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.grpc.client.service.GrpcMarginPositionService;
import io.bhex.broker.server.grpc.server.service.aspect.SiteFunctionLimitSwitchAnnotation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-12-22 18:02
 */
@Slf4j
@Service
public class MarginOrderService {

    @Resource
    SpotQuoteService spotQuoteService;

    @Autowired
    BasicService basicService;

    @Resource
    private AccountService accountService;

    @Resource
    private MarginService marginService;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private GrpcMarginPositionService grpcMarginPositionService;

    @Resource
    private OrderService orderService;

    private final String USDT_TOKEN = "USDT";

    //总资产
    public static final String ALL_POSITION_KEY = "allPosition";
    //总已借
    public static final String ALL_LOAN_KEY = "allLoan";

    public static final String ALL_UNPAID_KEY = "allUnpaid";

    private static final String MARGIN_CREATE_ORDER_LOCK_KEY = "MarginCreateOrderLock:%s_%s_%s";

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_COIN_TRADE_KEY,
            userSwitchGroupKey = BaseConfigConstants.FROZEN_USER_COIN_TRADE_GROUP)
    public BaseResult<CreateOrderResponse> createMarginOrderRisk(CreateOrderRequest request) {
        Long beginTime = System.currentTimeMillis();
        //本次下单风控计算使用到的指数
        Map<String, BigDecimal> indicesMap = new HashMap<>();
        Header header = request.getHeader();
        Long accountId = accountService.getMarginIndexAccountId(header, request.getAccountIndex());
        GetMarginPositionStatusReply positionStatusReply = marginService.getMarginPositionStatus(header, accountId);
        if (positionStatusReply.getCurStatus() == MarginCrossPositionStatusEnum.POSITION_FORCE_CLOSING) {
            log.warn("createMarginOrderRisk orgId:{} userId:{} accountId:{} status is FORCE_CLOSING cannot create order", header.getOrgId(), header.getUserId(), accountId);
            return BaseResult.fail(BrokerErrorCode.MARGIN_ACCOUNT_IS_FORCE_CLOSE);
        }
        SwitchStatus userSwitchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                BaseConfigConstants.FROZEN_USER_MARGIN_TRADE_GROUP, header.getUserId() + "");
        if (userSwitchStatus.isOpen()) {
            log.info("createMarginOrderRisk newMarginOrderSuspended org:{} userId:{}", header.getOrgId(), header.getUserId());
            return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED, "forbidden");
        }

        Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), request.getSymbolId());
        if (symbol == null) {
            log.warn("createMarginOrderRisk orgId:{} userId:{} accountId:{} cid:{} create order cannot find symbol {}",
                    header.getOrgId(), header.getUserId(), accountId, request.getClientOrderId(), request.getSymbolId());
            return BaseResult.fail(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
        }
        if (!symbol.getAllowMargin()) {
            log.warn("orgId:{} symbol:{} cannot margin trade", header.getOrgId(), symbol.getSymbolId());
            return BaseResult.fail(BrokerErrorCode.MARGIN_SYMBOL_NOT_TRADE);
        }
        boolean noLoan = false;
        CrossLoanPositionRequest loanRequest = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()))
                .setAccountId(accountId)
                .build();
        CrossLoanPositionReply reply = grpcMarginPositionService.getCrossLoanPosition(loanRequest);
        List<CrossLoanPosition> loanPosi = new ArrayList<>();
        if (reply != null) {
            loanPosi = reply.getCrossLoanPositionList().stream().filter(position -> DecimalUtil.toBigDecimal(position.getLoanTotal()).compareTo(BigDecimal.ZERO) > 0 || DecimalUtil.toBigDecimal(position.getInterestUnpaid()).compareTo(BigDecimal.ZERO) > 0)
                    .filter(position -> Stream.of(AccountService.TEST_TOKENS).noneMatch(testToken -> testToken.equalsIgnoreCase(position.getTokenId())))
                    .collect(Collectors.toList());
        }
        if (loanPosi.isEmpty()) { //没有借币
            if (request.getOrderType() == OrderType.MARKET) {
                //获取最新价
                BigDecimal lastPrice = getLastPrice(header, symbol, request.getOrderSide());
                BigDecimal qty;

                //市价额转量
                if (request.getOrderSide() == OrderSide.BUY) {
                    BaseResult<CreateOrderResponse> baseResult = orderService.validAmount(new BigDecimal(request.getQuantity()), symbol);
                    if (!baseResult.isSuccess()) {
                        return baseResult;
                    }
                    qty = new BigDecimal(request.getQuantity()).divide(lastPrice, new BigDecimal(symbol.getBasePrecision()).scale(), RoundingMode.DOWN);
                } else {
                    BaseResult<CreateOrderResponse> baseResult = orderService.validQuantity(new BigDecimal(request.getQuantity()), symbol);
                    if (!baseResult.isSuccess()) {
                        return baseResult;
                    }
                    qty = new BigDecimal(request.getQuantity());
                }
                request = request.toBuilder()
                        .setOrderType(OrderType.LIMIT)
                        .setTimeInForce(OrderTimeInForceEnum.IOC)
                        .setPrice(lastPrice.setScale(new BigDecimal(symbol.getMinPricePrecision()).scale(), RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setQuantity(qty.stripTrailingZeros().toPlainString())
                        .build();
            }
        } else {
            //判断是否是市价单
            if (request.getOrderType() == OrderType.MARKET) {
                //获取最新价
                BigDecimal lastPrice = getLastPrice(header, symbol, request.getOrderSide());
                BigDecimal qty;

                //市价额转量
                if (request.getOrderSide() == OrderSide.BUY) {
                    BaseResult<CreateOrderResponse> baseResult = orderService.validAmount(new BigDecimal(request.getQuantity()), symbol);
                    if (!baseResult.isSuccess()) {
                        return baseResult;
                    }
                    qty = new BigDecimal(request.getQuantity()).divide(lastPrice, new BigDecimal(symbol.getBasePrecision()).scale(), RoundingMode.DOWN);
                } else {
                    BaseResult<CreateOrderResponse> baseResult = orderService.validQuantity(new BigDecimal(request.getQuantity()), symbol);
                    if (!baseResult.isSuccess()) {
                        return baseResult;
                    }
                    qty = new BigDecimal(request.getQuantity());
                }
                //计算IOC价格
                BigDecimal iocPrice = calMarginIocPrice(header, symbol, qty, request.getOrderSide(), accountId, indicesMap, loanPosi);

                BigDecimal price;
                if (request.getOrderSide() == OrderSide.BUY) {
                    price = iocPrice.min(lastPrice);
                } else {
                    price = iocPrice.max(lastPrice);
                }
                request = request.toBuilder()
                        .setOrderType(OrderType.LIMIT)
                        .setTimeInForce(OrderTimeInForceEnum.IOC)
                        .setPrice(price.setScale(new BigDecimal(symbol.getMinPricePrecision()).scale(), RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setQuantity(qty.stripTrailingZeros().toPlainString())
                        .build();
            } else {
                BigDecimal price = new BigDecimal(request.getPrice());
                BigDecimal limitPrice = calMarginIocPrice(header, symbol, new BigDecimal(request.getQuantity()), request.getOrderSide(), accountId, indicesMap, loanPosi);
                if (request.getOrderSide() == OrderSide.BUY && price.compareTo(limitPrice) > 0) {
                    log.warn("margin order risk orgId:{} userId:{} symbol:{} clientOrderId:{} buy price {} greater than max limit price {}",
                            header.getOrgId(), header.getUserId(), symbol.getSymbolId(), request.getClientOrderId(), price, limitPrice);
                    // 买入价格大于买入限价
                    return BaseResult.fail(BrokerErrorCode.MARGIN_ORDER_PRICE_GREATER_LIMIT_FAILED);
                } else if (request.getOrderSide() == OrderSide.SELL && price.compareTo(limitPrice) < 0) {
                    log.warn("margin order risk orgId:{} userId:{} symbol:{} clientOrderId:{} sell price {} less than min limit price {}",
                            header.getOrgId(), header.getUserId(), symbol.getSymbolId(), request.getClientOrderId(), price, limitPrice);
                    // 卖出价格小于卖出限价
                    return BaseResult.fail(BrokerErrorCode.MARGIN_ORDER_PRICE_LESS_LIMIT_FAILED);
                }
            }
        }

        log.info("风控耗时：{}", System.currentTimeMillis() - beginTime);
        BaseResult<CreateOrderResponse> baseResult = orderService.newOrder(header, request.getAccountIndex(), symbol.getSymbolId(), request.getClientOrderId(), request.getOrderType(), request.getOrderSide(), request.getPrice(),
                request.getQuantity(), request.getTimeInForce(), request.getOrderSource(), request.getAccountType());
        return baseResult;
    }

    public BaseResult<CreatePlanSpotOrderResponse> createMarginPlanSpotOrder(CreatePlanSpotOrderRequest request) {
        if (request.getOrderType() != OrderType.LIMIT) {
            return BaseResult.fail(BrokerErrorCode.FEATURE_SUSPENDED);
        }
        //本次下单风控计算使用到的指数
        Map<String, BigDecimal> indicesMap = new HashMap<>();
        Header header = request.getHeader();
        //检查开关
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
        //获取用户accountId,账户类型校验
        int accountIndex = request.getAccountIndex();
        Long accountId;
        //杠杆账户判断当前账户是否处于强平, 强平不允许下任何订单
        accountId = accountService.getMarginIndexAccountId(header, accountIndex);
        GetMarginPositionStatusReply positionStatusReply = marginService.getMarginPositionStatus(header, accountId);
        if (positionStatusReply.getCurStatus() == MarginCrossPositionStatusEnum.POSITION_FORCE_CLOSING) {
            log.warn("orgId:{} userId:{} accountId:{} status is FORCE_CLOSING cannot create order", header.getOrgId(), header.getUserId(), accountId);
            return BaseResult.fail(BrokerErrorCode.MARGIN_ACCOUNT_IS_FORCE_CLOSE);
        }
        //检查入参
        String clientOrderId = request.getClientOrderId();
        OrderType orderType = request.getOrderType();
        OrderSide orderSide = request.getOrderSide();
        if (orderType == OrderType.UNKNOWN_ORDER_TYPE || orderSide == OrderSide.UNKNOWN_ORDER_SIDE) {
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
        if (!symbol.getAllowMargin()) {
            log.warn("orgId:{} symbol:{} cannot margin trade", header.getOrgId(), symbol.getSymbolId());
            return BaseResult.fail(BrokerErrorCode.MARGIN_SYMBOL_NOT_TRADE);
        }
        //获取借贷持仓
        CrossLoanPositionRequest loanRequest = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()))
                .setAccountId(accountId)
                .build();
        CrossLoanPositionReply reply = grpcMarginPositionService.getCrossLoanPosition(loanRequest);
        List<CrossLoanPosition> loanPosi = new ArrayList<>();
        if (reply != null) {
            loanPosi = reply.getCrossLoanPositionList().stream().filter(position -> DecimalUtil.toBigDecimal(position.getLoanTotal()).compareTo(BigDecimal.ZERO) > 0 || DecimalUtil.toBigDecimal(position.getInterestUnpaid()).compareTo(BigDecimal.ZERO) > 0)
                    .filter(position -> Stream.of(AccountService.TEST_TOKENS).noneMatch(testToken -> testToken.equalsIgnoreCase(position.getTokenId())))
                    .collect(Collectors.toList());
        }
        if (!loanPosi.isEmpty()) {
            //校验下单价格
            BigDecimal price = new BigDecimal(request.getPrice());
            BigDecimal limitPrice = calMarginIocPrice(header, symbol, new BigDecimal(request.getQuantity()), request.getOrderSide(), accountId, indicesMap, loanPosi);
            if (request.getOrderSide() == OrderSide.BUY && price.compareTo(limitPrice) > 0) {
                log.warn("margin order risk orgId:{} userId:{} symbol:{} clientOrderId:{} buy price {} greater than max limit price {}",
                        header.getOrgId(), header.getUserId(), symbol.getSymbolId(), request.getClientOrderId(), price, limitPrice);
                // 买入价格大于买入限价
                return BaseResult.fail(BrokerErrorCode.MARGIN_ORDER_PRICE_GREATER_LIMIT_FAILED);
            } else if (request.getOrderSide() == OrderSide.SELL && price.compareTo(limitPrice) < 0) {
                log.warn("margin order risk orgId:{} userId:{} symbol:{} clientOrderId:{} sell price {} less than min limit price {}",
                        header.getOrgId(), header.getUserId(), symbol.getSymbolId(), request.getClientOrderId(), price, limitPrice);
                // 卖出价格小于卖出限价
                return BaseResult.fail(BrokerErrorCode.MARGIN_ORDER_PRICE_LESS_LIMIT_FAILED);
            }
        }
        //下单
        return orderService.createPlanSpotOrder(request);
    }

    //获取行情最新价
    public BigDecimal getLastPrice(Header header, Symbol symbol, OrderSide side) {
        BigDecimal lastPrice = BigDecimal.ZERO;
        int flag = 1;
        //todo
        while (lastPrice.compareTo(BigDecimal.ZERO) <= 0 && flag <= 5) {
            lastPrice = spotQuoteService.getCacheCurrentPrice(symbol.getSymbolId(), symbol.getExchangeId(), header.getOrgId());
            flag++;
        }
        //为获取价格
        if (lastPrice.compareTo(BigDecimal.ZERO) <= 0) {
            //错误码后续更新
            log.warn("latest price is not exit {} {}", header.getOrgId(), symbol.getSymbolId());
            throw new BrokerException(BrokerErrorCode.MARGIN_LATEST_PRICE_NOT_EXIT);
        }
        if (side == OrderSide.BUY) {
            lastPrice = lastPrice.multiply(new BigDecimal("1.1"));
        } else {
            lastPrice = lastPrice.multiply(new BigDecimal("0.9"));
        }
        return lastPrice;
    }

    private BigDecimal calMarginIocPrice(Header header, Symbol symbol, BigDecimal qty, OrderSide side, Long accountId, Map<String, BigDecimal> indicesMap, List<CrossLoanPosition> loanPosi) {
        //获取借贷市值&持仓市值&借贷利息
        Map<String, BigDecimal> allPosiMap = calAllPositionAndLoan(header, header.getOrgId(), accountId, indicesMap, loanPosi);
        BigDecimal iocPrice;
        //获取风控线
        List<RiskConfig> riskConfigs = basicService.getOrgMarginRiskConfig(header.getOrgId());
        if (riskConfigs.isEmpty()) {
            //错误码后续更新
            log.warn(header.getOrgId() + " risk config is not exit");
            throw new BrokerException(BrokerErrorCode.MARGIN_RISK_CONFIG_NOT_EXIT);
        }
        BigDecimal stopLine = new BigDecimal(riskConfigs.get(0).getStopLine());
        //总持仓
        BigDecimal allPosi = allPosiMap.get(ALL_POSITION_KEY);
        //总已借
        BigDecimal allLoan = allPosiMap.get(ALL_LOAN_KEY);
        //总未还利息
        BigDecimal allUnpaid = allPosiMap.get(ALL_UNPAID_KEY);

        //买入币种指数价格U
        BigDecimal baseIndices;
        if (symbol.getBaseTokenId().equalsIgnoreCase(USDT_TOKEN)) {
            baseIndices = BigDecimal.ONE;
        } else {
            String buySymbol = symbol.getBaseTokenId() + USDT_TOKEN;
            baseIndices = getIndices(header.getOrgId(), buySymbol, indicesMap,symbol.getBaseTokenId());
        }
        //卖出币种指数价格U
        BigDecimal quoteIndices;
        if (symbol.getQuoteTokenId().equalsIgnoreCase(USDT_TOKEN)) {
            quoteIndices = BigDecimal.ONE;
        } else {
            String sellSymbol = symbol.getQuoteTokenId() + USDT_TOKEN;
            quoteIndices = getIndices(header.getOrgId(), sellSymbol, indicesMap,symbol.getQuoteTokenId());
        }
        if (side == OrderSide.BUY) {
            //买入限价 = （总持仓U-总未还利息U+买入币种数量*基础币种指数价格U-借贷市值U*平仓先）/(买入数量*计价币种指数价格U)
            iocPrice = (allPosi.subtract(allUnpaid).add(qty.multiply(baseIndices)).subtract(allLoan.multiply(stopLine)))
                    .divide(qty.multiply(quoteIndices), 18, RoundingMode.DOWN).multiply(new BigDecimal(0.99));

        } else {
            //卖出限价 = (借贷市值U*平仓线 - 总持仓U+总未还利息U +卖出币种数量*基础币种指数价格U)/（卖出数量*计价币种指数价格U）
            iocPrice = (allLoan.multiply(stopLine).subtract(allPosi).add(allUnpaid).add(qty.multiply(baseIndices)))
                    .divide(qty.multiply(quoteIndices), 18, RoundingMode.DOWN).multiply(new BigDecimal(1.01));
        }
        return iocPrice;
    }


    public Map<String, BigDecimal> calAllPositionAndLoan(Header header, Long orgId, Long accountId, Map<String, BigDecimal> indicesMap, List<CrossLoanPosition> loanPosi) {
        //总已借
        BigDecimal allLoan = BigDecimal.ZERO;
        //总未还利息
        BigDecimal allUnpaid = BigDecimal.ZERO;
        for (CrossLoanPosition posi : loanPosi) {
            //获取指数价格
            BigDecimal indices;
            if (posi.getTokenId().equalsIgnoreCase(USDT_TOKEN)) {
                indices = BigDecimal.ONE;
            } else {
                String symbol = posi.getTokenId() + USDT_TOKEN;
                indices = getIndices(orgId, symbol, indicesMap,posi.getTokenId());
            }

            BigDecimal loanValue = DecimalUtil.toBigDecimal(posi.getLoanTotal()).multiply(indices);
            allLoan = allLoan.add(loanValue);
            BigDecimal unpaidValue = DecimalUtil.toBigDecimal(posi.getInterestUnpaid()).multiply(indices);
            allUnpaid = allUnpaid.add(unpaidValue);
        }

        //获取总持仓
        List<Balance> balances = accountService.queryBalance(header, accountId, new ArrayList<>());
        BigDecimal allPosi = BigDecimal.ZERO;
        for (Balance balance : balances) {
            if (Stream.of(AccountService.TEST_TOKENS).anyMatch(testToken -> testToken.equalsIgnoreCase(balance.getTokenId()))) {
                continue;
            }
            BigDecimal indices;
            if (balance.getTokenId().equalsIgnoreCase(USDT_TOKEN)) {
                indices = BigDecimal.ONE;
            } else {
                String symbol = balance.getTokenId() + USDT_TOKEN;
                indices = getIndices(orgId, symbol, indicesMap,balance.getTokenId());
            }
            BigDecimal posiValue = new BigDecimal(balance.getTotal()).multiply(indices);
            allPosi = allPosi.add(posiValue);
        }

        Map<String, BigDecimal> map = new HashMap<>();
        map.put(ALL_POSITION_KEY, allPosi);
        map.put(ALL_LOAN_KEY, allLoan);
        map.put(ALL_UNPAID_KEY, allUnpaid);
        return map;
    }

    public BigDecimal getIndices(Long orgId, String symbol, Map<String, BigDecimal> indicesMap, String tokenId) {
        BigDecimal indices = null;
        if (!indicesMap.containsKey(symbol)) {
            int flag = 0;
            while (indices == null && flag <= 5) {
                indices = basicService.getIndices(symbol, orgId);
                flag++;
            }
            if (indices == null || indices.compareTo(BigDecimal.ZERO) <= 0) {
                log.warn("indices is not exit {} {}", orgId, symbol);
                Rate rate = basicService.getV3Rate(orgId, tokenId);
                if(rate == null){
                    log.warn("rate is not exit {} {}", orgId, tokenId);
                    throw new BrokerException(BrokerErrorCode.MARGIN_INDICES_NOT_EXIT);
                }
                indices = DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
            }
            indicesMap.put(symbol, indices);
        } else {
            indices = indicesMap.get(symbol);
        }
        return indices;

    }


}
