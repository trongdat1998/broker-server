package io.bhex.broker.server.domain;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class StatisticsUserLevelData {

    private Long id;

    private Long userId;

    private BigDecimal value;

    private Long spotAccountId;

    private Long contractAccountId;

    private Long optionAccountId;
}
