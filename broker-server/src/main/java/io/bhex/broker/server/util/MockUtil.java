package io.bhex.broker.server.util;

import io.bhex.base.account.BatchCancelOrderReply;
import io.bhex.base.account.CancelOrderReply;
import io.bhex.base.account.NewOrderReply;
import io.bhex.base.account.Order;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.proto.OrderStatusEnum;
import io.bhex.base.proto.OrderTypeEnum;
import io.bhex.base.token.SymbolInfo;
import io.bhex.base.token.TokenInfo;

public class MockUtil {

    private static Order.Builder createOrderBuilder() {
        TokenInfo baseToken = TokenInfo.newBuilder().setTokenId("BTC0524PS6900").setTokenName("BTC0524PS6900").build();
        TokenInfo quoteToken = TokenInfo.newBuilder().setTokenId("USDT").setTokenName("USDT").build();
        SymbolInfo symbolInfo = SymbolInfo.newBuilder()
                .setSymbolId("BTC0524PS6900")
                .setSymbolName("BTC0524PS6900")
                .setBaseToken(baseToken)
                .setQuoteToken(quoteToken)
                .build();

        return Order.newBuilder()
                .setCreatedTime(System.currentTimeMillis())
                .setOrderId(221669707124616960L)
                .setAccountId(122216245228131L)
                .setClientOrderId("1541161088265")
                .setSymbolId("BTC0524PS6900")
                .setSymbolName("BTC0524PS6900")
                .setBaseTokenId("BTC0524PS6900")
                .setQuoteTokenId("USDT")
                .setSymbol(symbolInfo)
                .setPrice(DecimalUtil.fromDouble(3300d))
                .setQuantity(DecimalUtil.fromDouble(1d))
                .setExecutedQuantity(DecimalUtil.fromDouble(0d))
                .setExecutedAmount(DecimalUtil.fromDouble(0d))
                .setAveragePrice(DecimalUtil.fromDouble(0d))
                .setType(OrderTypeEnum.LIMIT)
                .setSide(OrderSideEnum.BUY);
    }

    public static NewOrderReply createOrder() {
        return NewOrderReply.newBuilder()
                .setOrderId(221669707124616960L)
                .setOrder(createOrderBuilder().setStatus(OrderStatusEnum.FILLED).build())
                .setStatus(OrderStatusEnum.FILLED)
                .build();
    }

    public static CancelOrderReply cancelOrder() {
        return CancelOrderReply.newBuilder()
                .setClientOrderId("1541161088265")
                .setOrderId(221669707124616960L)
                .setOrder(createOrderBuilder().setStatus(OrderStatusEnum.CANCELED).build())
                .build();
    }

    public static BatchCancelOrderReply batchCancelOrder() {
        return BatchCancelOrderReply.newBuilder()
                .setOrderCount(10)
                .build();
    }
}
