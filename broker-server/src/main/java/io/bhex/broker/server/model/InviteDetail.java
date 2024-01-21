package io.bhex.broker.server.model;

import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_invite_detail")
public class InviteDetail {
    /**
     *
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     *
     */
    private Long orgId;
    /**
     *
     */
    private Long userId;


    private Long inviteUserId;
    /**
     * 1币币返佣转账类型 2期货返佣转账类型
     */
    private Integer type;

    /**
     * 获得币种
     */
    private String tokenId;
    /**
     * 获得币数
     */
    private BigDecimal getAmount;

    /**
     * 原始返佣金额
     */
    private BigDecimal amount;

    /**
     * 返佣比例
     */
    private BigDecimal rate;
    /**
     * 统计时间
     */
    private Long statisticsTime;

    /**
     * 状态 0 ： 有效  1：无效
     */
    private Integer status;
    /**
     *
     */
    private Date createdAt;
}
