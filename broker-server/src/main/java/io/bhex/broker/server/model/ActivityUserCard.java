package io.bhex.broker.server.model;

import io.bhex.broker.server.grpc.server.service.activity.ActivityConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Objects;


/**
 * tb_acitvity_user_card
 *
 * @author
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_activity_user_card")
public class ActivityUserCard implements Serializable, Comparable<ActivityUserCard> {
    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @Column(name = "org_id")
    private Long orgId;

    /**
     * 用户id
     */
    @Column(name = "user_id")
    @NotNull
    private Long userId;


    @Column(name = "activity_id")
    @NotNull
    private Long activityId;


    @Column(name = "card_id")
    @NotNull
    private Long cardId;

    /**
     * 发卡时间
     */
    @Column(name = "get_time")
    @NotNull
    private Timestamp getTime;

    /**
     * 过期时间
     */
    @Column(name = "expire_time")
    @NotNull
    private Timestamp expireTime;

    /**
     * 券状态，0=无效，1=未使用，2=已使用，3=过期
     */
    @Column(name = "card_status")
    @NotNull
    private Byte cardStatus;

    /**
     * 面值
     */
    @Column(name = "par_value")
    @NotNull
    private BigDecimal parValue;

    /**
     * 币种
     */
    @Column(name = "symbol")
    @NotNull
    private String symbol;

    @Column(name = "create_time")
    @NotNull
    private Timestamp createTime;

    @Column(name = "update_time")
    @NotNull
    private Timestamp updateTime;

    /**
     * 券类型，1=新手券，2=达人券，3=普照券
     */
    @Column(name = "card_type")
    @NotNull
    private Byte cardType;


    @Column(name = "used_time")
    private Timestamp usedTime;


    public Timestamp getUsedTimeOut() {
        if (Objects.isNull(this.usedTime) || usedTime.equals(this.createTime)) {
            return new Timestamp(0L);
        }

        return this.usedTime;
    }

    private static final long serialVersionUID = 1L;

    public void expire() {
        this.cardStatus = (byte) 3;
    }

    public boolean isValid() {
        if (Objects.isNull(cardStatus)) {
            return false;
        }

        return cardStatus.equals(ActivityConfig.UserCardStatus.VALID);
    }

    public boolean isExpectType(byte expeceCardType) {

        if (Objects.isNull(this.cardType)) {
            return false;
        }

        return this.cardType.equals(expeceCardType);

    }

    @Override
    public int compareTo(ActivityUserCard other) {
        return this.getExpireTime().compareTo(other.expireTime);
    }
}
