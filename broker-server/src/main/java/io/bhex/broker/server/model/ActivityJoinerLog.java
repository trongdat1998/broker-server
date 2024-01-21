package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;


@Data
@Table(name = "tb_activity_joiner_log")
public class ActivityJoinerLog {

    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @NotNull
    @Column(name = "user_id")
    private Long userId;

    @NotNull
    @Column(name = "follower_id")
    private Long followerId;

    @NotNull
    @Column(name = "group_id")
    private Long groupId;

    @NotNull
    @Column(name = "create_time")
    private Timestamp createTime;

}
