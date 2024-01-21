package io.bhex.broker.server.model;


import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.Objects;

@Data
@Table(name = "tb_trade_competition_result")
public class TradeCompetitionResult {

    @Id
    private Long id;

    private Long orgId;

    private String batch;

    private Long competitionId;

    private String competitionCode;

    private Long clientTransferId;

    private Long userId;

    private Long accountId;

    private BigDecimal rate;

    private String receiveTokenId;

    private BigDecimal receiveQuantity;

    private Integer status;

    private Integer transferStatus;

    private Date createTime;

    private Date updateTime;

    private BigDecimal currentContractBalance;

    private BigDecimal currentUnrealizedPnl;

    private BigDecimal startContractBalance;

    private BigDecimal startUnrealizedPnl;

    private String startBatch;

    private BigDecimal changedTotal;

    private BigDecimal changedTotalNegative;

    private BigDecimal incomeAmount;

    private BigDecimal currentTradeAmount;

    private String currentTradeAmountTokenId;


    @Transient
    public String getIncomeAmountSafe() {
        if (Objects.isNull(this.incomeAmount)) {
            return "0";
        }

        return this.incomeAmount.stripTrailingZeros().toPlainString();
    }

    @Transient
    public String getIncomeAmountSafeWithScale(int scale, RoundingMode roundingMode) {
        if (Objects.isNull(this.incomeAmount)) {
            return "0";
        }

        return this.incomeAmount.setScale(scale, roundingMode).stripTrailingZeros().toPlainString();
    }

    @Transient
    public String getRateSafe() {
        if (Objects.isNull(this.rate)) {
            return "0";
        }

        return this.rate.stripTrailingZeros().toPlainString();
    }

    @Transient
    public String getRateSafeWithScale(int scale, RoundingMode roundingMode) {
        if (Objects.isNull(this.rate)) {
            return "0";
        }

        return this.rate.setScale(scale, roundingMode).stripTrailingZeros().toPlainString();
    }
}
