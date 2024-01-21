package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @Description: 体验金活动
 * @Date: 2020/3/3 上午10:49
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Data
@Table(name = "tb_activity_experience_fund")
public class ActivityExperienceFund {

    public static final Integer STATUS_INIT = 0;
    public static final int STATUS_LOCK_ORG_BALANCE = 1;
    public static final Integer STATUS_ORG_TRANSFER_OUT_SUCCESS = 2;

    public static final Integer STATUS_ENDED = 3;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Integer type; //类型 1合约体验金

    private String title;

    private String description;

    private Long brokerId;

    private Long orgAccountId; //机构账户

    private Integer orgAccountType;

    private String tokenId; //空投币种
    private BigDecimal tokenAmount; //空投币种的数量

    private Integer redeemType; //赎回类型 0-不赎回 1-到期赎回

    private Long redeemTime; //赎回时间

    private BigDecimal redeemAmount; //赎回币种的数量

    private Long deadline; //过了这个日期 无论收回多少都线束任务

    private Integer userCount; //发送的用户总数

    private BigDecimal transferAssetAmount; //发送token总数


    private Integer status; //状态 0.初始化，未执行 1.赠送完毕 2.赠送失败  3.赎回成功 终态

    private String failedReason; //空投失败说明

    private String adminUserName;

    private Long createdAt;

    private Long updatedAt;

}
