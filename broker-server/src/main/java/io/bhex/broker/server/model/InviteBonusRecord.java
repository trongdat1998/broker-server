package io.bhex.broker.server.model;

import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * @author dudu
 * @date 2018-09-13
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_invite_bonus_record")
public class InviteBonusRecord {
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


    private Long transferId;
    /**
     * 活动类型
     */
    private Integer actType;

    /**
     * 获得币种
     */
    private String token;
    /**
     * 获得币数
     */
    private Double bonusAmount;
    /**
     * 统计时间
     */
    private Long statisticsTime;

    /**
     * 状态 0 ： 未支付 1：已支付
     */
    private Integer status;
    /**
     *
     */
    private Date createdAt;
    /**
     *
     */
    private Date updatedAt;

    // 1币币返佣转账类型 2期货返佣转账类型
    private Integer type;
}
