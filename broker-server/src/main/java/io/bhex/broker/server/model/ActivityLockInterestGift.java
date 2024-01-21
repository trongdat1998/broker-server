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
 * @CreateDate: 2019/7/16 3:31 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lock_interest_gift")
public class ActivityLockInterestGift {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private String projectId;
    private String receiveTokenId; // 获得货币 token id
    private String giftTokenId; // 赠送货币 token id
    private BigDecimal giftTokenQuantity; // 赠送比例 赠送货币货币数量
    private BigDecimal receiveTokenQuantity; // 赠送比例 获得货币数量
    private Integer giftToLock; // 赠送是否转入锁仓
    private Integer status;
    private Long createdTime;
    private Long updatedTime;
}
