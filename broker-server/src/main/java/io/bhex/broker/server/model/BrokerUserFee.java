package io.bhex.broker.server.model;


import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Table(name = "clear_rpt_broker_user_fee")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BrokerUserFee {

    private Date matchDate;

    private Long brokerId;

    private Long accountId;

    private String tokenId;

    private BigDecimal tokenFee;

    private Long userId;

    private BigDecimal amount;

    private BigDecimal quantity;

    private BigDecimal feeSys;

    private BigDecimal feeExchange;

    private BigDecimal feeMatchExchange;

    private BigDecimal feeBroker;

    private BigDecimal feeBrokerSpot;

    private BigDecimal feeBrokerContract;

    private BigDecimal makerFee;

    private BigDecimal amountSpot;

    private BigDecimal amountContract;
}
