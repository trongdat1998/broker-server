package io.bhex.broker.server.model;


import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Table(name = "rpt_broker_commission")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BrokerCommission {

    private Date dt;

    private BigDecimal commissionFeeUsdt;

    private BigDecimal marketFeeUsdt;

}
