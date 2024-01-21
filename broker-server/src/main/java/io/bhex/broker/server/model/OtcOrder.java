package io.bhex.broker.server.model;

import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

/**
 * 订单表
 *
 * @author lizhen
 * @see io.bhex.ex.otc.OTCOrderStatusEnum
 * @date 2018-09-11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order")
public class OtcOrder {
    /**
     * ID
     */
    @Id
    //@GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 交易所ID
     */
    private Long exchangeId;
    /**
     * 券商id
     */
    private Long orgId;
    /**
     * 订单id（交易所）
     */
    private Long orderId;
    /**
     * 订单类型 0-买入；1-卖出
     */
    private Integer side;
    /**
     * 商品id
     */
    private Long itemId;
    /**
     * maker id
     */
    private Long accountId;
    /**
     * 币种
     */
    private String tokenId;
    /**
     * 成交单价
     */
    private BigDecimal price;
    /**
     * 成交数量
     */
    private BigDecimal quantity;
    /**
     * 状态
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;

    /**
     * 共享深度标记，0=不共享，1=共享
     */
    //private Short depthShare;




}
