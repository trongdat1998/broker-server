package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "rpt_agent_commission")
public class AgentCommission {

    private Long id;

    private Date dt;

    private Long brokerId;

    private Long userId;

    private Long accountId;

    private String tokenId;

    private BigDecimal selfFeeSpot;

    private BigDecimal selfFeeContract;

    private BigDecimal agentFeeSpot;

    private BigDecimal agentFeeContract;

    private BigDecimal agentFee;

    private BigDecimal rateSpot;

    private BigDecimal rateContract;

    private Integer isAgent;
}
