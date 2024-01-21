package io.bhex.broker.server.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * tb_activity
 *
 * @author
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@Table(name = "tb_activity")
public class Activity implements Serializable {

    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 活动名字
     */
    @Column(name = "activity_name")
    @NotNull
    private String activityName;

    /**
     * 开始时间
     */
    @Column(name = "start_time")
    @NotNull
    private Timestamp startTime;

    /**
     * 结束时间
     */
    @Column(name = "end_time")
    @NotNull
    private Timestamp endTime;

    /**
     * 活动状态,1=有效,0=无效
     */
    @Column(name = "activity_status")
    @NotNull
    private Byte activityStatus;

    @Column(name = "create_time")
    @NotNull
    private Date createTime;

    @Column(name = "update_time")
    @NotNull
    private Date updateTime;

    /**
     * 预算
     */
    @Column(name = "budget")
    @NotNull
    private BigDecimal budget;

    /**
     * 余额
     */
    @Column(name = "balance")
    @NotNull
    private BigDecimal balance;

    /**
     * 币种
     */
    @Column(name = "token")
    @NotNull
    private String token;

    /**
     * 账户id
     */
    @Column(name = "account_id")
    @NotNull
    private Long accountId;

    @Column(name = "org_id")
    @NotNull
    private Long orgId;

    private static final long serialVersionUID = 1L;


    public boolean isValid() {

        if (Objects.isNull(this.activityStatus) || Objects.isNull(startTime)
                || Objects.isNull(endTime)) {
            return false;
        }

        if (this.activityStatus.equals((byte) 0)) {
            return false;
        }

        Timestamp now = Timestamp.from(new Date().toInstant());
        if (startTime.after(now)) {
            return false;
        }

        if (endTime.before(now)) {
            return false;
        }

        return true;
    }

    public boolean balanceGreaterThan(BigDecimal expend) {
        if (Objects.isNull(expend) || Objects.isNull(this.balance)) {
            throw new NullPointerException();
        }

        return this.balance.compareTo(expend) == 1;
    }
}
