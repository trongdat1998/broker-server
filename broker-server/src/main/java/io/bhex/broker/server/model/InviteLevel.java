package io.bhex.broker.server.model;

import com.google.common.collect.Lists;
import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * @author dudu
 * @date 2018-09-13
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_invite_level")
public class InviteLevel {
    /**
     *
     */
    @Id
    private Long id;

    /**
     * 活动的ID
     */
    private Long actId;
    /**
     * 等级
     */
    private Integer level;
    /**
     * 等级标签
     */
    private String levelTag;
    /**
     * 等级条件
     */
    private Integer levelCondition;

    /**
     * 持币条件
     */
    private Double tokenCondition;
    /**
     * 直接费率
     */
    private BigDecimal directRate;

    /**
     * 持币直接费率
     */
    private Double tokenDirectRate;
    /**
     * 间接费率
     */
    private BigDecimal indirectRate;
    /**
     *
     */
    private Date createdAt;
    /**
     *
     */
    private Date updatedAt;


    public static List<InviteLevel> getInviteLevelTemplate(Long actId) {

        InviteLevel level1 = InviteLevel.builder()
                .actId(actId)
                .level(1)
                .levelTag("励志青铜")
                .levelCondition(0)
                .tokenCondition(0D)
                .directRate(BigDecimal.ZERO)
                .tokenDirectRate(0D)
                .indirectRate(BigDecimal.ZERO)
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();

        InviteLevel level2 = InviteLevel.builder()
                .actId(actId)
                .level(2)
                .levelTag("璀璨白银")
                .levelCondition(0)
                .tokenCondition(0D)
                .directRate(BigDecimal.ZERO)
                .tokenDirectRate(0D)
                .indirectRate(BigDecimal.ZERO)
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();

        InviteLevel level3 = InviteLevel.builder()
                .actId(actId)
                .level(3)
                .levelTag("荣耀黄金")
                .levelCondition(0)
                .tokenCondition(0D)
                .directRate(BigDecimal.ZERO)
                .tokenDirectRate(0D)
                .indirectRate(BigDecimal.ZERO)
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();

        InviteLevel level4 = InviteLevel.builder()
                .actId(actId)
                .level(4)
                .levelTag("尊尚铂金")
                .levelCondition(0)
                .tokenCondition(0D)
                .directRate(BigDecimal.ZERO)
                .tokenDirectRate(0D)
                .indirectRate(BigDecimal.ZERO)
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();
        InviteLevel level5 = InviteLevel.builder()
                .actId(actId)
                .level(5)
                .levelTag("永恒钻石")
                .levelCondition(0)
                .tokenCondition(0D)
                .directRate(BigDecimal.ZERO)
                .tokenDirectRate(0D)
                .indirectRate(BigDecimal.ZERO)
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();

        InviteLevel level6 = InviteLevel.builder()
                .actId(actId)
                .level(6)
                .levelTag("无上至尊")
                .levelCondition(0)
                .tokenCondition(0D)
                .directRate(BigDecimal.ZERO)
                .tokenDirectRate(0D)
                .indirectRate(BigDecimal.ZERO)
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();

        List<InviteLevel> levelList = Lists.newArrayList();
        levelList.add(level1);
        levelList.add(level2);
        levelList.add(level3);
        levelList.add(level4);
        levelList.add(level5);
        levelList.add(level6);

        return levelList;
    }

}
