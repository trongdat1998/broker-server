package io.bhex.broker.server.domain.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * redis lock return
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingLockResult {
    private Boolean rtn;
    private String key;
}
