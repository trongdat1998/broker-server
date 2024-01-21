package io.bhex.broker.server.model;

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
@Table(name = "tb_user_level_config")
public class UserLevelConfig {

    public static final int PREVIEW_STATUS = 2;
    public static final int OK_STATUS = 1;
    public static final int DELETED_STATUS = -1;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private String levelIcon;
    private String levelValue; //级别名称 多语言配置用json表示
    private Long levelPosition; //级别排序，小值在前面
    private String levelCondition; //待级配置，用字符串表示，使用时要解析

    //权益折扣数据库中存储的是 0.9， 代表：原价*90%
    @Builder.Default
    private BigDecimal spotBuyMakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal spotBuyTakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal spotSellMakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal spotSellTakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal optionBuyMakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal optionBuyTakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal optionSellMakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal optionSellTakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal contractBuyMakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal contractBuyTakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal contractSellMakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal contractSellTakerDiscount = BigDecimal.ONE;
    @Builder.Default
    private BigDecimal withdrawUpperLimitInBtc = BigDecimal.ZERO;
    @Builder.Default
    private Integer cancelOtc24hWithdrawLimit = 0; //1-是 0-否
    @Builder.Default
    private Integer isBaseLevel = 0;
    @Builder.Default
    private Integer inviteBonusStatus = 1;

    private Integer status;
    private Long created;
    private Long updated;

    private Long statDate;
    private Integer leaderStatus; //队长专用标志
}
