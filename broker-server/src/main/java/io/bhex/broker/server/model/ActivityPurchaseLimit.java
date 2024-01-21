package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Objects;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/7/23 11:28 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_purchase_limit")
public class ActivityPurchaseLimit {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private Long projectId;
    private Integer verifyKyc;
    private Integer verifyBalance;
    private Integer verifyBindPhone;
    private Integer verifyAvgBalance;
    private String levelLimit;

    /**
     * value 为 Json。{"tokenId": limitNum, "BHT": 500, "USDT": 100}
     * 条件为或的关系
     */
    private String balanceRuleJson;
    private Long createdTime;
    private Long updatedTime;

    public boolean verifyKycBool() {
        if (Objects.isNull(verifyKyc)) {
            return false;
        }

        return verifyKyc.equals(1);
    }

    public boolean verifyBalanceBool() {
        if (Objects.isNull(verifyBalance)) {
            return false;
        }

        return verifyBalance.equals(1);
    }

    public boolean verifyBindPhoneBool() {
        if (Objects.isNull(verifyBindPhone)) {
            return false;
        }

        return verifyBindPhone.equals(1);
    }
}
