package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-12-22 15:35
 */
@Data
@Table(name = "tb_margin_daily_risk_statistics")
public class MarginDailyRiskStatistics {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;
    private Long date;
    //平均风险度
    private BigDecimal averageSafety;
    //借贷市值(BTC)
    private BigDecimal loanValue;
    //借贷市值(USDT)
    private BigDecimal usdtLoanValue;
    //总市值折合(BTC)
    private BigDecimal allValue;
    //总市值折合(USDT)
    private BigDecimal usdtAllValue;
    // 用户人数
    private Integer userNum;
    //有效借贷人数
    private Integer loanUserNum;
    //有效借贷笔数
    private Integer loanOrderNum;
    //当日有效借贷笔数
    private Integer todayLoanOrderNum;
    //当日还款笔数
    private Integer todayPayNum;
    //当日借款人数
    private Integer todayLoanUserNum;

    private Long created;
    private Long updated;

}
