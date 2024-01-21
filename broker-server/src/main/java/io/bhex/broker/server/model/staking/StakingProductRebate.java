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
@Table(name = "tb_staking_product_rebate")
public class StakingProductRebate {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 机构
     */
    private Long orgId;

    /**
     * 理财产品ID
     */
    private Long productId;

    /**
     * 理财产品类型
     */
    private Integer productType;

    /**
     * 币种
     */
    private String tokenId;

    /**
     * 派息日期
     */
    private Long rebateDate;

    /**
     * 类型 0=利息 1=本金
     */
    private Integer type;

    /**
     * 利息计算方式，0=利率(默认) 1=金额分摊
     */
    private Integer rebateCalcWay;

    /**
     * 派息利率
     */
    private BigDecimal rebateRate;

    /**
     * 派息金额
     */
    private BigDecimal rebateAmount;

    /**
     * 第几期
     */
    private Integer numberOfPeriods;

    /**
     * 状态:0=待派息 1=已派息
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
