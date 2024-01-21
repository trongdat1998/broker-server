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
@Table(name = "tb_staking_asset")
public class StakingAsset {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 机构ID
     */
    private Long orgId;

    /**
     * 账户ID
     */
    private Long accountId;

    /**
     * 理财产品ID
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
     * 总申购金额
     */
    private BigDecimal totalAmount;

    /**
     * 当前持有金额
     */
    private BigDecimal currentAmount;

    /**
     * 昨日收益
     */
    private BigDecimal lastProfit;

    /**
     * 总收益
     */
    private BigDecimal totalProfit;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * 更新时间
     */
    private Long updatedAt;

    private transient BigDecimal btcValue;

    private transient BigDecimal usdtValue;


}
