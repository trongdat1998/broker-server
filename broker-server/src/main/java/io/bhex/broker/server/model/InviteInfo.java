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
@Table(name = "tb_invite_info")
public class InviteInfo {
    /**
     *
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * 用户ID
     */
    private Long userId;
    /**
     * 账户ID
     */
    private Long accountId;
    /**
     * 邀请总数
     */
    private Integer inviteCount;
    /**
     * 有效邀请数量
     */
    private Integer inviteVaildCount;

    /**
     * 直接有效数量
     */
    private Integer inviteDirectVaildCount;

    /**
     * 间接有效数量
     */
    private Integer inviteIndirectVaildCount;

    /**
     * 邀请队长数量
     */
    private Integer inviteHobbitLeaderCount;
    /**
     * 邀请等级
     */
    private Integer inviteLevel;
    /**
     * 直接邀请费率
     */
    private Double directRate;
    /**
     * 间接邀请费率
     */
    private Double indirectRate;
    /**
     * 获得币数
     */
    private Double bonusCoin;
    /**
     * 获得点卡数
     */
    private Double bonusPoint;
    /**
     *
     */
    private Date createdAt;
    /**
     *
     */
    private Date updatedAt;

    /**
     *  是否在邀请时隐藏邀请码 0不隐藏 1隐藏
     */
    private Integer hide;
}
