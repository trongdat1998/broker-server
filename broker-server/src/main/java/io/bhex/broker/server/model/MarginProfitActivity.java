package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-03-09 15:26
 */
@Data
@Table(name = "tb_margin_profit_activity")
public class MarginProfitActivity {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long joinDate;

    private Long orgId;

    private Long userId;

    private Long accountId;

    private Long submitTime;

    private Integer kycLevel;

    private BigDecimal profitRate;

    private BigDecimal allPositionUsdt;

    private BigDecimal todayPositionUsdt;

    private Integer joinStatus;

    private Integer dayRanking;

    private Long updates;

    private Long created;

    private Long updated;
}
