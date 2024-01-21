package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;


@Data
@Table(name = "tb_activity_welfare_log")
public class ActivityWelfareCardLog {

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
    @Column(name = "member_number")
    private Integer memberNumber;

    @NotNull
    @Column(name = "create_time")
    private Timestamp createTime;
}
