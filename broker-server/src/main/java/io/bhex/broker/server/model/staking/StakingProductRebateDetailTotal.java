package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 单次派息设置的派息合计
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingProductRebateDetailTotal {
    private Integer rebateType;
    private BigDecimal amount;
    private String tokenId;
}
