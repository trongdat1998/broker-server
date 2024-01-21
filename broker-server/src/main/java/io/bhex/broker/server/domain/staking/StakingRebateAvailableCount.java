package io.bhex.broker.server.domain.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 统计理财产品为计息的计息配置数量
 *
 * @author songxd
 * @date 2020-10-21
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StakingRebateAvailableCount {
    private Long productId;
    private Integer availableCount;
}
