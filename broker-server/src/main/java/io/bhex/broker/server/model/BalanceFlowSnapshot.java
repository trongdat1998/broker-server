package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;
import java.math.BigDecimal;
import java.sql.Timestamp;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "ods_balance_flow")
public class BalanceFlowSnapshot {

    private Long id;

    private Long balanceFlowId;

    private Long balanceId;

    private Integer businessSubject;

    private Long subjectExtId;

    private Long accountId;

    private String tokenId;

    private BigDecimal changed;

    private BigDecimal total;

    private Long ticketId;

    private String shardId;

    private Long offsetId;

    private Timestamp createdAt;
}
