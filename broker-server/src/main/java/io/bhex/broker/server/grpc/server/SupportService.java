package io.bhex.broker.server.grpc.server;

import com.google.common.collect.Lists;
import io.bhex.base.account.*;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.proto.OrderStatusEnum;
import io.bhex.base.proto.OrderTypeEnum;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.order.Fee;
import io.bhex.broker.grpc.order.Order;
import io.bhex.broker.grpc.order.*;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcOrderService;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ProjectName: io.bhex.broker.server.grpc.server
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2018/12/30 下午3:39
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */

@Service
@Slf4j
public class SupportService {

    @Autowired
    private UserVerifyMapper userVerifyMapper;

    @Autowired
    private AccountMapper accountMapper;

    @Autowired
    private GrpcOrderService grpcOrderService;

    @Resource
    private GrpcBalanceService grpcBalanceService;

    @Resource
    private LoginLogMapper loginLogMapper;

    @Resource
    private UserMapper userMapper;

    /**
     * 获取用户信息
     *
     * @return map
     */
    public Map<String, Object> checkUserInfo(long orgId, long uid, String symbolId) {
        Map<String, Object> userInfo = new HashMap<>();

        User user = userMapper.getByUserId(uid);
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }

        //注册时间
        userInfo.put("registerTime", user.getCreated() == null ? "" : new DateTime(user.getCreated()).toString("yyyy-MM-dd HH:mm:ss"));

        //accountId
        Account account = this.accountMapper.getMainAccount(orgId, uid);
        if (account == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        userInfo.put("accountId", account == null ? "" : account.getAccountId());

        //获取用户kyc状态
        UserVerify userVerify
                = userVerifyMapper.getKycPassUserByUserId(uid);
        userInfo.put("isKyc", userVerify == null ? true : false);

        // 最近20 BHT/BTC tradeDetail
        GetTradesRequest tradeDetailRequest = GetTradesRequest
                .newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(account.getAccountId())
                .setSymbolId(symbolId)
                .setLimit(20)
                .build();

        // 获取用户的成交记录
        GetTradesReply getTradesReply = grpcOrderService.queryMatchInfo(tradeDetailRequest);
        List<MatchInfo> matchInfos = new ArrayList<>();
        if (getTradesReply.getTradesList() != null && getTradesReply.getTradesList().size() > 0) {
            getTradesReply.getTradesList().forEach(trade -> matchInfos.add(getMatchInfo(trade)));
        }
        userInfo.put("tradeDetail", matchInfos);

        // balanceDetail
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(account.getAccountId())
                .build();
        BalanceDetailList balanceDetailList = grpcBalanceService.getBalanceDetail(request);
        List<Balance> balanceDetail = Lists.newArrayList();
        if (balanceDetailList != null) {
            balanceDetail = balanceDetailList.getBalanceDetailsList().stream().map(this::getBalance).collect(Collectors.toList());
        }
        userInfo.put("balanceDetail", balanceDetail);

        //最后三次登录时间 和 ip
        List<LoginLog> loginLogs
                = loginLogMapper.queryLastLoginLogs(uid, 3);
        userInfo.put("lastLoginLogs", loginLogs);

        //获取挂单数据
        GetOrdersRequest.Builder builder = GetOrdersRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(account.getAccountId())
                .setLimit(20)
                .addAllOrderStatusList(Arrays.asList(OrderStatusEnum.NEW))
                .addAllSide(Arrays.asList(OrderSideEnum.BUY, OrderSideEnum.SELL));
        GetOrdersRequest getOrdersRequest = builder.build();
        GetOrdersReply reply = grpcOrderService.queryOrders(getOrdersRequest);
        List<Order> orderList = Lists.newArrayList();
        if (reply.getOrdersList() != null && reply.getOrdersList().size() > 0) {
            reply.getOrdersList().forEach(order -> orderList.add(getOrder(order)));
        }

        userInfo.put("orders", orderList);
        return userInfo;
    }


    private MatchInfo getMatchInfo(Trade trade) {
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
        return MatchInfo.newBuilder()
                .setAccountId(trade.getAccountId())
                .setOrderId(trade.getOrderId())
                .setTradeId(trade.getTradeId())
                .setSymbolId(trade.getSymbol().getSymbolId())
                .setSymbolName(trade.getSymbol().getSymbolName())
                .setBaseTokenId(trade.getSymbol().getBaseToken().getTokenId())
                .setBaseTokenName(trade.getSymbol().getBaseToken().getTokenName())
                .setQuoteTokenId(trade.getSymbol().getQuoteToken().getTokenId())
                .setQuoteTokenName(trade.getSymbol().getQuoteToken().getTokenName())
                .setPrice(DecimalUtil.toBigDecimal(trade.getPrice()).stripTrailingZeros().toPlainString())
                .setQuantity(DecimalUtil.toBigDecimal(trade.getQuantity()).stripTrailingZeros().toPlainString())
                .setFee(Fee.newBuilder()
                        .setFeeTokenId(trade.getTradeFee().getToken().getTokenId())
                        .setFeeTokenName(trade.getTradeFee().getToken().getTokenName())
                        .setFee(DecimalUtil.toBigDecimal(trade.getTradeFee().getFee()).stripTrailingZeros().toPlainString())
                        .build()
                )
                .setOrderType(orderType)
                .setOrderSide(OrderSide.valueOf(trade.getSide().name()))
                .setTime(trade.getMatchTime())
                .setIsMaker(trade.getIsMaker())
                .build();
    }

    private Balance getBalance(BalanceDetail balanceDetail) {
        return Balance.newBuilder()
                .setTokenId(balanceDetail.getToken().getTokenId())
                .setTokenName(balanceDetail.getToken().getTokenName())
                .setFree(DecimalUtil.toBigDecimal(balanceDetail.getAvailable()).stripTrailingZeros().toPlainString())
                .setLocked(DecimalUtil.toBigDecimal(balanceDetail.getLocked()).stripTrailingZeros().toPlainString())
                .setTotal(DecimalUtil.toBigDecimal(balanceDetail.getTotal()).stripTrailingZeros().toPlainString())
//                .setBtcValue(DecimalUtil.toBigDecimal(balanceDetail.getTotal()).multiply(btcExchangeRate)
//                        .setScale(18, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .build();
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
            default:
                orderType = OrderType.MARKET;
                break;
        }
        return Order.newBuilder()
                .setAccountId(order.getAccountId())
                .setOrderId(order.getOrderId())
                .setClientOrderId(order.getClientOrderId())
                .setSymbolId(order.getSymbol().getSymbolId())
                .setSymbolName(order.getSymbol().getSymbolName())
                .setBaseTokenId(order.getSymbol().getBaseToken().getTokenId())
                .setBaseTokenName(order.getSymbol().getBaseToken().getTokenName())
                .setQuoteTokenId(order.getSymbol().getQuoteToken().getTokenId())
                .setQuoteTokenName(order.getSymbol().getQuoteToken().getTokenName())
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
                .build();
    }
}
