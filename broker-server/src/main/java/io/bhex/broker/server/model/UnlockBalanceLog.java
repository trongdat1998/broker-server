package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Table(name = "tb_unlock_balance_log")
public class UnlockBalanceLog {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @Column(name="broker_id")
    private Long brokerId;

    @Column(name="lock_id")
    private Long lockId;

    @Column(name="user_id")
    private Long userId;

    @Column(name="account_id")
    private Long accountId;

    @Column(name="type")
    private Integer type;

    @Column(name="status")
    private Integer status;

    @Column(name="token_id")
    private String tokenId;

    @Column(name="amount")
    private BigDecimal amount;

    @Column(name = "operator")
    private Long operator;

    @Column(name="client_order_id")
    private Long clientOrderId;

    @Column(name="subject_type")
    private Integer subjectType;

    @Column(name="mark")
    private String mark;

    @Column(name="create_time")
    private Date createTime;

    @Column(name="update_time")
    private Date updateTime;
}
