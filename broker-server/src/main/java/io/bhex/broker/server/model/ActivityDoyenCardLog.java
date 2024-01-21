package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;


@Data
@Table(name = "tb_activity_doyen_log")
public class ActivityDoyenCardLog {

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
    @Column(name = "owner_id")
    private Long ownerId;

    @NotNull
    @Column(name = "friend_id")
    private Long friendId;

    @NotNull
    @Column(name = "create_time")
    private Timestamp createTime;

    @NotNull
    @Column(name = "update_time")
    private Timestamp updateTime;
}
