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
@Table(name = "tb_staking_product_order")
public class StakingProductOrder {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 机构ID
     */
    private Long orgId;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 账户ID
     */
    private Long accountId;

    /**
     * 理财商品ID
     */
    private Long productId;

    /**
     * 产品类型 0定期 1定期锁仓 2活期
     */
    private Integer productType;

    /**
     * 转账流水号
     */
    private Long transferId;

    /**
     * 购买手数
     */
    private Integer payLots;

    /**
     * 支付金额
     */
    private BigDecimal payAmount;

    /**
     * 订单类型 0=申购 1=赎回 2=赎回还本
     */
    private Integer orderType;

    /**
     * 币种
     */
    private String tokenId;

    /**
     * 生效计息日期
     */
    private Long takeEffectDate;

    /**
     * 赎回日期
     */
    private Long redemptionDate;

    /**
     * 最新派息日期
     */
    private Long lastInterestDate;

    /**
     * 到期是否自动转活期
     */
    private Integer canAutoRenew;

    /**
     * 状态: 0=申购中 1=申购成功 2=申购失败
     */
    private Integer status;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * 修改时间
     */
    private Long updatedAt;


}
