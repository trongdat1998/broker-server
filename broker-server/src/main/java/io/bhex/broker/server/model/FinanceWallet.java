package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_finance_wallet")
public class FinanceWallet {

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
     * 赎回中
     */
    private BigDecimal redeem;

    /**
     * 昨日收益
     */
    private BigDecimal lastProfit;

    /**
     * 总收益
     */
    private BigDecimal totalProfit;

    private Long createdAt;

    private Long updatedAt;

    private transient BigDecimal btcValue;

    private transient BigDecimal usdtValue;

}
