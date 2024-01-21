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
 * @CreateDate: 2019/5/28 10:03 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lamb_interest")
public class ActivityLambInterest {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private Long projectId;
    private Long accountId; //accountId
    private Long rateId;   // 费率id
    private BigDecimal fixedInterestRate; // 固定利率（活动开始后的奖励费率）冗余，以便对账使用
    private BigDecimal floatingRate;  // 浮动利率（每日矿工费）
    private String orderIds;  // 对应申购记录ids
    private BigDecimal interestAmount; // 总利息数
    private BigDecimal lambLockedAmount; // 持仓数
    private Long settlementTime; // 结算时间
    private Integer status;
    private Long createdTime;
    private Long updatedTime;
}
