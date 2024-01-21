package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-03-06 16:16
 */
@Data
@Table(name = "tb_margin_open_activity")
public class MarginOpenActivity {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long userId;

    private Long accountId;

    private Long submitTime;

    private Integer kycLevel;

    private BigDecimal allPositionUsdt;

    private BigDecimal todayPositionUsdt;

    private BigDecimal monthPositionUsdt;

    private Integer joinStatus;

    private Integer dayWinning = 0;

    private Integer monthWinning = 0;

    private Long created;

    private Long updated;
}
