package io.bhex.broker.server.model;


import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Builder
@Data
@Table(name = "tb_activity_doyenCard_unlock_log")
@EqualsAndHashCode
public class ActivityDoyenCardUnlockLog {

    @EqualsAndHashCode.Exclude
    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "follower_id")
    private Long followerId;

    @Column(name = "create_time")
    private Timestamp createTime;
}
