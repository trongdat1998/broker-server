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
@Table(name = "tb_staking_pool_rebate_detail")
public class StakingPoolRebateDetail {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 机构ID
     */
    private Long orgId;
    /**
     * 第几期派息
     */
    private Long stakingRebateId;
    /**
     * 持币返利项目ID
     */
    private Long stakingId;
    /**
     * 矿池类型
     */
    private Integer stakingType;
    /**
     * 矿池购买订单ID
     */
    private Long orderId;
    /**
     * 用户ID
     */
    private Long userId;
    /**
     * 用户账户ID
     */
    private Long userAccountId;
    /**
     * 支出账户ID
     */
    private Long originAccountId;
    /**
     * 转账流水号
     */
    private Long transferId;
    /**
     * 派息时客户持仓:数据来源于快照
     */
    private BigDecimal userPosition;
    /**
     * 返币数量
     */
    private BigDecimal rebateCoinAmount;
    /**
     * 状态 0=待转账 1=已转账 2=已取消
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
