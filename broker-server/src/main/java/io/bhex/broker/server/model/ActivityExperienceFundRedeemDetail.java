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
@Table(name = "tb_activity_experience_fund_redeem_detail")
public class ActivityExperienceFundRedeemDetail {

    public static final Integer INIT_STATUS = 0;
    public static final Integer TRANSFER_SUC_STATUS = 1;
    public static final Integer RELEASE_RISK_BALANCE_SUC_STATUS = 2;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long transferRecordId;

    private BigDecimal redeemAmount;

    private Integer status; //0-init 1-transfer suc 2-release suc

    private Long createdAt;

    private Long updatedAt;
}
