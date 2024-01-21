package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class OrgBalanceSummary {

    private String tokenId;
    private BigDecimal totalSum;
    private BigDecimal availableSum;
    private BigDecimal lockedTotal;

}
