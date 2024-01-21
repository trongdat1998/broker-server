package io.bhex.broker.server.model;

import lombok.*;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Builder
@Data
@Table(name = "tb_activity_sendCardLog_daily")
@EqualsAndHashCode
public class ActivitySendCardLogDaily {


    @EqualsAndHashCode.Exclude
    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "user_identity")
    private String identity;

    @EqualsAndHashCode.Exclude
    @Column(name = "card_number")
    private Integer cardNumber;

    @Column(name = "send_day")
    private Long sendDay;

    @EqualsAndHashCode.Exclude
    @Column(name = "create_time")
    private Timestamp createTime;

    @EqualsAndHashCode.Exclude
    @Column(name = "update_time")
    private Timestamp updateTime;

    @EqualsAndHashCode.Exclude
    @Column(name = "version")
    private Integer version;
}
