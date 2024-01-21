package io.bhex.broker.server.model;

/**
 * @Description: 体验金活动发放记录表
 * @Date: 2020/3/3 上午11:45
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Table(name = "tb_activity_experience_fund_transfer_record")
public class ActivityExperienceFundTransferRecord {

    public static final Integer INIT_STATUS = 0; //初始状态
    public static final Integer TRANSFER_IN_STATUS = 1; //运营账户转到用户
    public static final Integer ADD_RISK_BALANCE_STATUS = 2; //

    public static final Integer TRANSFER_OUT_STATUS = 3; //已赎回完毕
    public static final Integer RELEASE_RISK_BALANCE_STATUS = 4; //终态


    @Id
    private Long id;

    private Long brokerId;

    private Long activityId;

    private Long userId;

    private Long accountId;

    private String tokenId;

    private BigDecimal tokenAmount;

    private BigDecimal redeemAmount;

    private Integer execRedeemTimes; //执行赎回次数

    private Integer status; // 0-初始状态 1-已转账到用户 2-开始赎回 3-已赎回完毕 4-终态

    private Long createdAt;

    private Long updatedAt;
}
