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
@Table(name = "tb_staking_product")
public class StakingProduct {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 机构id
     */
    private Long orgId;

    /**
     * 币种ID
     */
    private String tokenId;

    /**
     * 派息方式:1=分期付息 2=一次性还本付息
     */
    private Integer dividendType;

    /**
     * 派息方式值:如果是一次性付本还息,则为 1,如果是分期付息,到期付本,则这里保存的是分几期
     */
    private Integer dividendTimes;

    /**
     * 发行期限:一般为 7/14/30/60/90
     */
    private Integer timeLimit;

    /**
     * 参考年化收益率,可能是一个范围值
     */
    private String referenceApr;

    /**
     * 实际年化利率,定期和活动和参考年化是一样的值
     */
    private BigDecimal actualApr;

    /**
     * 7日年化利率
     */
    private BigDecimal weeklyApr;

    /**
     * 每用户最低申购手
     */
    private Integer perUsrLowLots;

    /**
     * 每用户最大申购手
     */
    private Integer perUsrUpLots;

    /**
     * 发行最大手
     */
    private Integer upLimitLots;

    /**
     * 展示发行最大手
     */
    private Integer showUpLimitLots;

    /**
     * 每手金额
     */
    private BigDecimal perLotAmount;

    /**
     * 已售手
     */
    private Integer soldLots;

    /**
     * 申购开始日期
     */
    private Long subscribeStartDate;

    /**
     * 申购结束日期
     */
    private Long subscribeEndDate;

    /**
     * 计息开始日期
     */
    private Long interestStartDate;

    /**
     * 排序值
     */
    private Integer sort;

    /**
     * 类型:0=定期 1=活期 2=锁仓
     */
    private Integer type;

    /**
     * 状态:0=下架 1=上架
     */
    private Integer isShow;

    /**
     * 本金账户
     */
    private Long principalAccountId;

    /**
     * 派息账户ID
     */
    private Long dividendAccountId;

    /**
     * 资金流:1=转账 2=锁仓
     */
    private Integer fundFlow;

    /**
     * 推荐显示位置
     */
    private Integer arrposid;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * 更新时间
     */
    private Long updatedAt;


}
