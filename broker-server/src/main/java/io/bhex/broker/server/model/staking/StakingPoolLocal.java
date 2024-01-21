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
@Table(name = "tb_staking_pool_local")
public class StakingPoolLocal {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 机构ID
     */
    private Long orgId;
    /**
     * 持仓生息/矿池ID
     */
    private Long stakingId;
    /**
     * 项目名称
     */
    private String stakingName;
    /**
     * 语言
     */
    private String language;
    /**
     * 项目详情
     */
    private String details;
    /**
     * 创建时间
     */
    private Long createdAt;
    /**
     * 修改时间
     */
    private Long updatedAt;

}
