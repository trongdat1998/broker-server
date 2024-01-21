package io.bhex.broker.server.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeCompetitionRankInfo {

    private Long userId;

    private String email;

    private BigDecimal rate;
}

