package io.bhex.broker.server.model;

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
@Table(name = "tb_finance_interest_data")
public class FinanceInterestData {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long userId;

    private Long accountId;

    /**
     * 金额产品ID
     */
    private Long productId;

    /**
     * 币种
     */
    private String token;

    /**
     * 计息余额
     */
    private BigDecimal balance;

    /**
     * 申购中
     */
    private BigDecimal purchase;

    /**
     * 原始利率
     */
    private BigDecimal originalRate;

    /**
     * 发放利率
     */
    private BigDecimal rate;

    /**
     * 计息日期
     */
    private String statisticsTime;

    private Long createdAt;

    private Long updatedAt;

}
