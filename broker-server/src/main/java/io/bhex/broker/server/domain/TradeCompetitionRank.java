package io.bhex.broker.server.domain;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class TradeCompetitionRank {

    private Long userId;

    private BigDecimal rate;

}
