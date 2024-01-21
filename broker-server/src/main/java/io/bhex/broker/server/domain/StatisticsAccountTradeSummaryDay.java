package io.bhex.broker.server.domain;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class StatisticsAccountTradeSummaryDay {

    private String dt;
    private Long brokerId;
    private Long accountId;
    private Integer tradeType;
    private String symbolId;
    private  String brokerUserId;
    private BigDecimal buyQuantity;
    private BigDecimal buyAmount;
    private BigDecimal buyAmountUsdt;
    private BigDecimal buyAmountBtc;
    private BigDecimal buyAmountUsd;
    private Integer buyOrderNum;
    private BigDecimal sellQuantity;
    private BigDecimal sellAmount;
    private BigDecimal sellAmountUsdt;
    private BigDecimal sellAmountBtc;
    private BigDecimal sellAmountUsd;
    private Integer sellOrderNum;
    private BigDecimal buyFeeTotal;
    private BigDecimal buyFeeTotalUsdt;
    private BigDecimal buyFeeTotalBtc;
    private String buyFeeTotalUsd;
    private BigDecimal sellFeeTotal;
    private BigDecimal sellFeeTotalUsdt;
    private BigDecimal sellFeeTotalBtc;
    private BigDecimal sellFeeTotalUsd;
    private String buyFeeToken;
    private String sellFeeToken;
}
