package io.bhex.broker.server.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;
import lombok.Data;

/**
 * tb_activity_card
 *
 * @author
 */
@Data
@Table(name = "tb_activity_card")
public class ActivityCard implements Serializable {

    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 券名字
     */
    @Column(name = "card_name")
    @NotNull
    private String cardName;

    /**
     * 有效天数
     */
    @Column(name = "validity_days")
    @NotNull
    private Integer validityDays;

    /**
     * 券面值
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

    /**
     * 分配基数
     */
    @Column(name = "base_assign")
    @NotNull
    private Integer baseAssign;

    /**
     * 活动id
     */
    @Column(name = "activity_id")
    @NotNull
    private Long activityId;

    /**
     * 券类型，1=新手券，2=达人券，3=普照券
     */
    @Column(name = "card_type")
    @NotNull
    private Short cardType;

    /**
     * 状态，0=无效，1=有效
     */
    @Column(name = "card_status")
    @NotNull
    private Byte cardStatus;

    @Column(name = "description")
    private String desc;

    @Column(name = "create_time")
    @NotNull
    private Date createTime;

    @Column(name = "update_time")
    @NotNull
    private Date updateTime;

    public String getDesc() {
        return Strings.nullToEmpty(this.desc);
    }

    private static final long serialVersionUID = 1L;

    public boolean isValid() {
        if (Objects.nonNull(this.cardStatus) && this.cardStatus.equals((byte) 1)) {
            return true;
        }

        return false;
    }
}
