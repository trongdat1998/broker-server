package io.bhex.broker.server.model;

import io.bhex.ex.otc.OTCOrderStatusEnum;
import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Objects;

/**
 * 订单表
 *
 * @author lizhen
 * @date 2018-09-11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_unconfirm")
public class UnconfirmOtcOrder {
    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 券商id
     */
    private Long orgId;
    /**
     * 订单id（交易所）
     */
    private Long orderId;
    /**
     * 账户id
     */
    private Long accountId;
    /**
     * 状态
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Long createDate;
    /**
     * 修改时间
     */
    private Long updateDate;

    private Byte notified;


    public OTCOrderStatusEnum getStatusEnum(){
        return OTCOrderStatusEnum.forNumber(status);
    }


    public boolean getNotifiedBoolean(){
        if(Objects.isNull(notified)){
            return false;
        }

        return notified.equals((byte)1);
    }
}
