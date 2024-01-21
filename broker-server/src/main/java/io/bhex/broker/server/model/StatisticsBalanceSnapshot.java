package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatisticsBalanceSnapshot {

    private Long orgId;
    private String statisticsTime;
    private Long userId;
    private Long accountId;
    private String tokenId;
    private BigDecimal total;
    private String activityTime;

}
