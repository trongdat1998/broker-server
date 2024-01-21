package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_finance_product")
public class FinanceProduct {

    @Id
    private Long id;

    private Long orgId;

    /**
     * 币种
     */
    private String token;

    /**
     * 类型 0：活期 1：定期
     */
    private Integer type;

    /**
     * 平台总限额
     */
    private BigDecimal totalLimit;

    /**
     * 平台每日限额
     */
    private BigDecimal dailyLimit;

    /**
     * 用户每日限额
     */
    private BigDecimal userLimit;

    /**
     * 交易精度
     */
    private BigDecimal tradeScale;

    /**
     * 交易最小金额
     */
    private BigDecimal tradeMinAmount;

    /**
     * 利息精度
     */
    private BigDecimal interestScale;

    /**
     * 利息最小金额
     */
    private BigDecimal interestMinAmount;

    /**
     * 赎回限制金额
     */
    private BigDecimal redeemLimitAmount;

    /**
     * 七日年化
     */
    private BigDecimal sevenYearRate;

    /**
     * 是否自动转账
     */
    private Integer autoTransfer;

    private Integer sort;

    private Integer showInHomePage;

    /**
     * 开售时间
     */
    private Long startTime;

    /**
     * 执行时间
     */
    private Long executeTime;

    /**
     * 是否允许申购
     */
    private Integer allowPurchase;

    /**
     * 是否允许赎回
     */
    private Integer allowRedeem;

    /**
     * 状态
     */
    private Integer status;

    private Long createdAt;

    private Long updatedAt;

}
