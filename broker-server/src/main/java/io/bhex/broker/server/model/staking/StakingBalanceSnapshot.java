package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingBalanceSnapshot {
    private Long id;
    private Long orgId;
    private String statisticsTime;
    private Long userId;
    private Long accountId;
    private String tokenId;
    private BigDecimal total;
    private String activityTime;
}
