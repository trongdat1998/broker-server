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
@Table(name = "tb_invite_bonus_detail")
public class InviteBonusDetail {
    /**
     *
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     *
     */
    private Long userId;
    /**
     * 被邀请人ID
     */
    private Long invitedId;

    /**
     * 被邀请人名称
     */
    private String invitedName;

    private Integer actType;
    /**
     * 统计时间标识20180912
     */
    private Long statisticsTime;
    /**
     * 贡献币数
     */
    private Double contributeCoin;
    /**
     * 贡献点卡数量
     */
    private Double contributePoint;
    /**
     * 状态  0：未入账  1：已入账
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
}
