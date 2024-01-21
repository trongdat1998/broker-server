package io.bhex.broker.server.domain.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 活期资产和订单合计金额，用于资产和订单校验
 * @author
 * @date
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StakingAssetOrderAmount {
    private Long orgId;
    private Long productId;
    private Long userId;
    private BigDecimal orderAmount;
    private BigDecimal assetAmount;
}
