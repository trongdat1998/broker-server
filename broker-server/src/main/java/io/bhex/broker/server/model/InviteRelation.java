package io.bhex.broker.server.model;

import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @author dudu
 * @date 2018-09-13
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_invite_relation")
public class InviteRelation {
    /**
     *
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 券商id
     */
    private Long orgId;
    /**
     * 用户ID
     */
    private Long userId;
    /**
     * 被邀请人ID
     */
    private Long invitedId; // invitedUserId

    /**
     * 被邀请人账户ID
     */
    @Deprecated
    private Long invitedAccountId;
    /**
     * 被邀请人名称
     */
    @Deprecated
    private String invitedName;
    /**
     * 被邀请类型  1：直接  2：间接
     */
    private Integer invitedType;
    /**
     * 邀请状态  0：无效  1：有效
     */
    private Integer invitedStatus;

    /**
     * 被邀请用户是队长
     */
    private Integer invitedHobbitLeader;

    /**
     * 返佣的费率
     */
    private BigDecimal rate;

    /**
     * 贡献币数
     */
    private Double contributeCoin;
    /**
     * 贡献点卡数
     */
    private Double contributePoint;
    /**
     *
     */
    private Date createdAt;
    /**
     *
     */
    private Date updateAt;
}
