package io.bhex.broker.server.message;

import com.google.common.base.Strings;
import com.google.common.primitives.Longs;

import java.math.BigDecimal;

import io.bhex.base.account.Trade;
import io.bhex.base.proto.DecimalUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ProjectName:
 * @Package:
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2020-04-02 16:55
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageTradeDetail {
    private Long tradeId;
    private Long accountId;
    private Long ticketId;
    private Long orderId;
    private Long matchOrderId;
    private String symbolId;
    private Integer side;
    private BigDecimal quantity;
    private BigDecimal amount;
    private BigDecimal price;
    private BigDecimal tokenFee;
    private BigDecimal sysTokenFee;
    private String sysTokenId;
    private Long matchTime;
    private Boolean isMaker;
    private Integer orderType;
    private String baseTokenId;
    private String quoteTokenId;
    private String feeTokenId;
    private BigDecimal fee;
    private String tradeFeeId;
    private BigDecimal tradeFee;
    private BigDecimal pnl; // 合约平仓盈亏
    private Long orgId;
    private Long brokerUserId;
    private Long matchOrgId;
    private Long matchUserId;
    private Long matchAccountId;
    private Integer accountType;
    private Boolean isNormal;
    private Boolean isClose;
    private Long createdAt;


    public static MessageTradeDetail messageBuild(Trade trade) {
        return MessageTradeDetail.builder()
                .tradeId(trade.getTradeId())
                .accountId(trade.getAccountId())
                .ticketId(trade.getTicketId())
                .orderId(trade.getOrderId())
                .matchOrderId(trade.getMatchOrderId())
                .symbolId(trade.getSymbolId())
                .side(trade.getSide().getNumber())
                .quantity(DecimalUtil.toBigDecimal(trade.getQuantity()))
                .amount(DecimalUtil.toBigDecimal(trade.getAmount()))
                .price(DecimalUtil.toBigDecimal(trade.getPrice()))
                .tokenFee(DecimalUtil.toBigDecimal(trade.getTokenFee()))
                .sysTokenFee(DecimalUtil.toBigDecimal(trade.getSysTokenFee()))
                .sysTokenId(trade.getSysTokenId())
                .matchTime(trade.getMatchTime())
                .isMaker(trade.getIsMaker())
                .orderType(trade.getOrderType().getNumber())
                .baseTokenId(trade.getBaseTokenId())
                .quoteTokenId(trade.getQuoteTokenId())
                .feeTokenId(trade.getFeeTokenId())
                .fee(DecimalUtil.toBigDecimal(trade.getFee()))
                .tradeFeeId(trade.getTradeFee().getToken().getTokenId())
                .tradeFee(DecimalUtil.toBigDecimal(trade.getTradeFee().getFee()))
                .pnl(DecimalUtil.toBigDecimal(trade.getPnl()).stripTrailingZeros())
                .orgId(trade.getOrgId())
                .brokerUserId(trade.getBrokerUserId())
                .matchOrgId(trade.getOrgId() == trade.getMatchOrgId() ?
                        trade.getMatchOrgId() : 0)
                .matchUserId(trade.getOrgId() == trade.getMatchOrgId() ?
                        (Strings.isNullOrEmpty(trade.getMatchBrokerUserId()) ? 0L : Longs.tryParse(trade.getMatchBrokerUserId())) : 0)
                .matchAccountId(trade.getMatchAccountId())
                .accountType(trade.getAccountType())
                .isNormal(trade.getIsNormal())
                .isClose(trade.getIsClose())
                .build();
    }

}
