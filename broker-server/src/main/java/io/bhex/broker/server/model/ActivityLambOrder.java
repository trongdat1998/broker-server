package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/5/28 9:39 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lamb_order")
public class ActivityLambOrder {

    public final static Integer STATUS_INIT = 0;
    public final static Integer STATUS_PAY_SUCCESS = 1;
    public final static Integer STATUS_PAY_FAILED = 2;

    public final static Integer LOCKED_STATUS_INIT = 0;
    public final static Integer LOCKED_STATUS_SUCCESS = 1;
    public final static Integer LOCKED_STATUS_FAILED = 2;
    public final static Integer LOCKED_STATUS_UNLOCKED = 3;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private Long clientOrderId;
    private Long accountId;
    private Long projectId;
    private BigDecimal amount;
    private String tokenId;
    private Integer status;  //支付状态 0 待支付 1 成功 2 失败
    private Integer lockedStatus;  //锁仓状态状态 0 未锁仓 1 锁仓成功 2 锁仓失败 3 解锁并返回给用户
    private Long purchaseTime; //购买下单时间
    private Long createdTime;
    private Long updatedTime;
}
