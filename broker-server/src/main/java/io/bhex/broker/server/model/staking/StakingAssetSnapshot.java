package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * 资产快照
 * @author generator
 * @date 2020-08-21
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_asset_snapshot")
public class StakingAssetSnapshot {

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
     * 理财产品ID
     */
    private Long productId;

    /**
     * 币种
     */
    private String tokenId;

    /**
     * 日期:yyyy-MM-dd
     */
    private Long dailyDate;

    /**
     * 净资产:计息金额
     */
    private BigDecimal netAsset;

    /**
     * 当日派息
     */
    private BigDecimal payInterest;

    /**
     * 年化利率
     */
    private BigDecimal apr;

    /**
     * 状态 0=待派息 1=已派息
     */
    private Integer status;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * 更新时间
     */
    private Long updatedAt;

    /**
     * 派息设置
     */
    private Long productRebateId;

}
