package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_pool_org_map")
public class StakingPoolOrgMap {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 机构id
     */
    private Long orgId;
    /**
     * 持仓项目ID
     */
    private Long stakingId;
    /**
     * 收息和派息账户
     */
    private Long interestAccountId;
    /**
     * 券商提成比率
     */
    private BigDecimal deductRate;
    /**
     * 状态: 1=开通
     */
    private Integer status;
    /**
     * 创建日期
     */
    private Long createdAt;
    /**
     * 修改日期
     */
    private Long updatedAt;

}
