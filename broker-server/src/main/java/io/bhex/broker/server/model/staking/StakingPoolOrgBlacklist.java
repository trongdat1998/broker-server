package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_pool_org_blacklist")
public class StakingPoolOrgBlacklist {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 持仓生息项目ID
     */
    private Long stakingId;
    /**
     * 机构ID串
     */
    private String orgIdStr;

    /**
     * 创建日期
     */
    private Long createdAt;
    /**
     * 修改日期
     */
    private Long updatedAt;

}
