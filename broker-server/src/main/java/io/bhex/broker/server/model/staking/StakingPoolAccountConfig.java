package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_pool_account_config")
public class StakingPoolAccountConfig {
    /**
     *
     **/
    private Long id;
    /**
     * 类型:0=持仓生币 1=锁仓
     */
    private Integer stakingType;
    /**
     * 券商id
     */
    private Long orgId;
    /**
     * 本金账户
     */
    private Long principalAccountId;
    /**
     * 派息账户
     */
    private Long dividendAccountId;

}
