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
@Table(name = "tb_staking_product_rebate_detail")
public class StakingProductRebateDetail {

    @Id
    @GeneratedValue(generator = "JDBC")
    /**
     **/ private Long id;

    /**
     * 机构ID
     */
    private Long orgId;

    /**
     * 理财商品派息设置ID
     */
    private Long productRebateId;

    /**
     * 理财项目ID
     */
    private Long productId;

    /**
     * 产品类型
     */
    private Integer productType;

    /**
     * 币种
     */
    private String tokenId;

    /**
     * 理财购买订单ID
     */
    private Long orderId;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 转入用户账户ID
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
     * 返利类型:1=利息 2=本金
     */
    private Integer rebateType;

    /**
     * 返利金额
     */
    private BigDecimal rebateAmount;

    /**
     * 状态 0=待转账 1=已转账
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
