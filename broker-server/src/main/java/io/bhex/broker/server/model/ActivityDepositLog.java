package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * tb_activity_deposit_log
 *
 * @author
 */
@Data
@Table(name = "tb_activity_deposit_log")
public class ActivityDepositLog implements Serializable, Comparable<ActivityDepositLog> {
    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 充值记录id
     */
    @Column(name = "data_id")
    @NotNull
    private Long dataId;

    @Column(name = "user_id")
    @NotNull
    private Long userId;

    @Column(name = "amount")
    @NotNull
    private BigDecimal amount;

    @Column(name = "token_name")
    @NotNull
    private String tokenName;

    @Column(name = "btc_value")
    @NotNull
    private BigDecimal btcValue;

    /**
     * 充值时间
     */
    @Column(name = "deposit_time")
    @NotNull
    private Timestamp depositTime;


    /**
     * 记录时间
     */
    @Column(name = "create_time")
    @NotNull
    private Timestamp createTime;

    @Column(name = "update_time")
    @NotNull
    private Timestamp updateTime;

    //0=未处理，1=已处理
    @Column(name = "record_status")
    @NotNull
    private Short status;

    private static final long serialVersionUID = 1L;

    @Override
    public int compareTo(ActivityDepositLog other) {
        return this.getBtcValue().compareTo(other.getBtcValue());
    }


    public static class STATUS {
        public static final short VALID = 0;
        public static final short INVALID = 1;
    }
}
