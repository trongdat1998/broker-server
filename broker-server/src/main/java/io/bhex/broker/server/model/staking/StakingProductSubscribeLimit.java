package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_product_subscribe_limit")
public class StakingProductSubscribeLimit {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 券商id
     */
    private Long orgId;
    /**
     * 项目id
     */
    private Long productId;

    /**
     * 业务类型
     */
    private Integer businessType;

    /**
     * 是否校验kyc 0不校验 1校验
     */
    private Integer verifyKyc;
    /**
     * 是否校验绑定手机
     */
    private Integer verifyBindPhone;
    /**
     * 是否校验持仓资产 0不校验 1校验
     */
    private Integer verifyBalance;
    /**
     * 是否校验平均持仓
     */
    private Integer verifyAvgBalance;
    /**
     * 持仓限制，value 为 Json。{"tokenId": limitNum, "BHT": 500, "USDT": 100}。或的关系。
     */
    private String balanceRuleJson;
    /**
     * 等级门槛
     */
    private String levelLimit;
    /**
     * 创建时间
     */
    private Long createdTime;
    /**
     * 修改时间
     */
    private Long updatedTime;


}
