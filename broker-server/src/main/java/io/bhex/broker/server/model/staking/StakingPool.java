package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_pool")
public class StakingPool {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 券商机构id
     */
    private Long orgId;

    /**
     * 币种
     */
    private String tokenId;

    /**
     * 利息发放币种,默认同持仓币种一致
     */
    private String interestTokenId;

    /**
     * 参考年化收益率,可能是一个范围值,范围值中间用逗号分隔
     */
    private String referenceApr;

    /**
     * 最新利率
     */
    private BigDecimal latestRate;

    /**
     * 用户最低持仓
     */
    private BigDecimal lowPosition;

    /**
     * 用户平均持仓,扩展
     */
    private BigDecimal avgPosition;

    /**
     * 用户最大持仓,扩展
     */
    private BigDecimal maxPosition;

    /**
     * 用户当前持仓:扩展
     */
    private BigDecimal curPosition;

    /**
     * 总返币数量
     */
    private BigDecimal totalRebateCoinAmount;

    /**
     * 申购开始日期
     */
    private Long subscribeStartDate;

    /**
     * 申购结束日期
     */
    private Long subscribeEndDate;

    /**
     * 开始计息日期
     */
    private Long interestStartDate;

    /**
     * 标签备注
     */
    private String tags;

    /**
     * 排序值
     */
    private Integer sort;

    /**
     * 类型:1=持仓生币 2=锁仓
     */
    private Integer type;

//    /**
//     * 本金账户ID
//     */
//    private Long principalAccountId;
//
//    /**
//     * 支出派息账户ID
//     */
//    private Long dividendAccountId;

    /**
     * 平台提成比率
     */
    private BigDecimal deductRate;

    /**
     * 派息周期:日=1 周=2 月=3
     */
    private Integer rebateCycle;

    /**
     * 计算利息方式 0=年化利率 1=固定额度
     */
    private Integer calcInterestType;

    /**
     * 状态:0=下架 1=上架
     */
    private Integer isShow;

    /**
     * 对客户kyc是否有要求
     */
    private Integer needKyc;

    /**
     * vip级别,是否对客户vip有要求
     */
    private Integer vipLevel;

    /**
     * 推荐位置:扩展
     */
    private Integer arrposid;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * '更新时间'
     */
    private Long updatedAt;

}
