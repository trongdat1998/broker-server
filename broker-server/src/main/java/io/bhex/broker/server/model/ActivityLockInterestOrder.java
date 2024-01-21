package io.bhex.broker.server.model;

import java.math.BigDecimal;
import java.util.Objects;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/6/4 3:04 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lock_interest_order")
public class ActivityLockInterestOrder {

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
    private Long userId;
    private Long projectId;
    private String projectCode;
    private BigDecimal amount;
    private String tokenId;
    private String tokenName;
    private Integer status;  //支付状态 0 待支付 1 成功 2 失败
    private Integer lockedStatus;  //锁仓状态状态 0 未锁仓 1 锁仓成功 2 锁仓失败 3 解锁并返回给用户

    private Long purchaseTime; //购买下单时间
    private Long createdTime;
    private Long updatedTime;

    private Integer type;// 1锁仓派息  2IEO 3 抢购 4 自由模式

    private BigDecimal userAmount;//用户获得的币
    private String userTokenId;//用户获得token
    private Integer userTransferStatus;//用户账户转账状态  0未处理 1成功 2失败
    private Long userClientOrderId;//用户转账幂等ID

    private BigDecimal sourceAmount;//运营账户获得的币
    private Integer sourceTransferStatus;//运营账户转账状态  0未处理 1成功 2失败
    private Long sourceAccountId;//运营账户ID
    private String sourceTokenId;//运营账户获得的token
    private Long ieoAccountClientOrderId;//运营账户空投幂等ID

    private Integer giftOpen;//是否有赠送 0无 1有
    private BigDecimal giftAmount;//赠送金额
    private String giftTokenId;//赠送token id
    private Integer giftTransferStatus;//赠送转账状态 0未处理 1成功 2失败
    private Long giftClientOrderId;//赠送空投幂等ID
    private Integer isLock;//是否到锁仓

    public String amountToString() {
        if (Objects.isNull(amount)) {
            return "0";
        }

        return amount.stripTrailingZeros().toPlainString();
    }

}
