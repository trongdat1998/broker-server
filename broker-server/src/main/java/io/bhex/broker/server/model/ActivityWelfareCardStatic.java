package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.sql.Timestamp;


@Data
@Table(name = "tb_activity_welfare_static")
public class ActivityWelfareCardStatic {

    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @NotNull
    @Column(name = "activity_id")
    private Long activityId;

    @NotNull
    @Column(name = "card_id")
    private Long cardId;

    @NotNull
    @Column(name = "group_id")
    private Long groupId;

    @NotNull
    @Column(name = "total_member")
    private Integer totalMember;

    @NotNull
    @Column(name = "par_value")
    private BigDecimal parValue;

    @NotNull
    @Column(name = "symbol")
    private String symbol;

    @NotNull
    @Column(name = "create_time")
    private Timestamp createTime;

    @NotNull
    @Column(name = "update_time")
    private Timestamp updateTime;
}
