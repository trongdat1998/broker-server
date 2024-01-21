package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_discount_fee_config")
public class DiscountFeeConfig {

    @Id
    private Long id;

    private Long orgId;

    private Long exchangeId;

    private String symbolId;

    private Integer type;

    private BigDecimal coinTakerBuyFeeDiscount;

    private BigDecimal coinTakerSellFeeDiscount;

    private BigDecimal coinMakerBuyFeeDiscount;

    private BigDecimal coinMakerSellFeeDiscount;

    private BigDecimal optionTakerBuyFeeDiscount;

    private BigDecimal optionTakerSellFeeDiscount;

    private BigDecimal optionMakerBuyFeeDiscount;

    private BigDecimal optionMakerSellFeeDiscount;

    private BigDecimal contractTakerBuyFeeDiscount;

    private BigDecimal contractTakerSellFeeDiscount;

    private BigDecimal contractMakerBuyFeeDiscount;

    private BigDecimal contractMakerSellFeeDiscount;

    private String name;

    private String mark;

    private Integer status;

    private Date created;

    private Date updated;
}
