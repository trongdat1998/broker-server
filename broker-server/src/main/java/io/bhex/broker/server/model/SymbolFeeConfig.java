package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
@Table(name = "tb_symbol_fee_config")
public class SymbolFeeConfig {

    @Id
    private Long id;

    private Long orgId;

    private Long exchangeId;

    private String symbolId;

    private BigDecimal takerBuyFee;

    private BigDecimal takerSellFee;

    private BigDecimal makerBuyFee;

    private BigDecimal makerSellFee;

    private Integer status;

    private Integer category;

    private String baseTokenId;

    private String baseTokenName;

    private String quoteTokenId;

    private String quoteTokenName;

    private Date created;

    private Date updated;
}
