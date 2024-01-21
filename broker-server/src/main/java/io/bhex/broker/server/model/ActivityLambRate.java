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
 * @CreateDate: 2019/5/28 10:00 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lamb_rate")
public class ActivityLambRate {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private Long projectId;
    private BigDecimal fixedInterestRate; // 固定利率（活动开始后的奖励费率）
    private BigDecimal floatingRate; //浮动利率（每日矿工费）
    private Integer status; //生效 失效
    private Long enableTime;
    private Long createdTime;
    private Long updatedTime;
}
