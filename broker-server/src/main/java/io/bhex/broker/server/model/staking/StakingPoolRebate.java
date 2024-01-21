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
@Table(name = "tb_staking_pool_rebate")
public class StakingPoolRebate {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    /**
     * 持币返利项目ID
     */
    private Long stakingId;

    /**
     * 类型
     */
    private Integer stakingType;

    /**
     * 币种
     */
    private String tokenId;

    private Long depositRecordId;

    /**
     * 派息日期
     */
    private Long rebateDate;
    /**
     * 返币数量
     */
    private BigDecimal rebateCoinAmount;
    /**
     * 折算年化利率
     */
    private BigDecimal apr;
    /**
     * 状态 0=未派息 1=已派息 2=已取消,在派息日期前可以修改派息数据
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
