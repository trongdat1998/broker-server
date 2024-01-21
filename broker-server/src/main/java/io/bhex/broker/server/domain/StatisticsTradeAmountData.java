package io.bhex.broker.server.domain;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class StatisticsTradeAmountData {

    private Integer tradeType;

    private BigDecimal amount;
}
