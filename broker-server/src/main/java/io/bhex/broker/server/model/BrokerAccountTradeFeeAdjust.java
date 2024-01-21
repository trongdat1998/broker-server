package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_broker_account_trade_fee_adjust")
@Slf4j
@Builder(builderClassName = "Builder", toBuilder = true)
public class BrokerAccountTradeFeeAdjust {

    private Long id;

    private Long brokerId;

    private Long accountId;

    private BigDecimal makerFeeRateAdjust;

    private BigDecimal makerRewardToTakerRateAdjust;

    private BigDecimal takerFeeRateAdjust;

    private BigDecimal takerRewardToMakerRateAdjust;

    private Integer deleted;
}
