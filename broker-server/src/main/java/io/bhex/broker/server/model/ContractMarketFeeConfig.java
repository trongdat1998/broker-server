package io.bhex.broker.server.model;


import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Table(name = "tb_contract_market_fee_config")
public class ContractMarketFeeConfig {

    @Id
    private Long id;
    private Long orgId;
    private Long exchangeId;
    private String symbol;
    private BigDecimal makerBuyFeeRate;
    private BigDecimal makerSellFeeRate;
    private BigDecimal takerBuyFeeRate;
    private BigDecimal takerSellFeeRate;
    private Integer status;
    private Date created;
    private Date updated;
}
