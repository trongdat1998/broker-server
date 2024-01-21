package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "clear_rpt_broker_user_fee")
public class StatisticsRpcBrokerUserFee {

//    private String match_date;
//    private Long brokerId;
    private Long accountId;
    private String tokenId;
    private BigDecimal tokenFee;
//    private Long userId;
//    private BigDecimal amount;
//    private BigDecimal quantity;
//    private BigDecimal feeSys;
//    private BigDecimal feeExchange;
//    private BigDecimal feeMatchExchange;
//    private BigDecimal feeBroker;
//    private BigDecimal feeBrokerSpot;
//    private BigDecimal feeBrokerContract;
//    private BigDecimal makerFee;
//    private BigDecimal tokenFeeSpot;
//    private BigDecimal tokenFeeContract;
//    private BigDecimal makerBonus;
//    private BigDecimal amountSpot;
//    private BigDecimal amountContract;
}
