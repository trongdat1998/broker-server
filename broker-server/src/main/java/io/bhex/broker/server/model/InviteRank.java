package io.bhex.broker.server.model;

import lombok.*;

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
@Table(name = "tb_invite_rank")
public class InviteRank {
    /**
     *
     */
    @Id
    private Long id;
    /**
     *
     */
    private Long userId;

    /**
     * 用户名称
     */
    private String userName;
    /**
     * 月份标记   201809
     */
    private Long month;
    /**
     * 排名类型 0：币  1：点卡
     */
    private Integer type;
    /**
     * 数量
     */
    private Double amount;
    /**
     * 状态 0:正常 -1：删除
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
