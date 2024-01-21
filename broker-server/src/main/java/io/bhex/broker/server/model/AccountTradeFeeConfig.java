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
@Builder(toBuilder = true, builderClassName = "Builder")
@Table(name = "tb_account_trade_fee_config")
public class AccountTradeFeeConfig {
    @Id
    private Long id;
    private Long orgId;
    private transient Long config_id; // ignore
    private Long exchangeId;
    private String symbolId;
    private Long userId;
    private Long accountId;
    private BigDecimal makerBuyFeeRate;
    private BigDecimal makerSellFeeRate;
    private BigDecimal takerBuyFeeRate;
    private BigDecimal takerSellFeeRate;
    private Integer status;
    private Date created;
    private Date updated;
}
