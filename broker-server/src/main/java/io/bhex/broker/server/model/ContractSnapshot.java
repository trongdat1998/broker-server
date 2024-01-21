package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "rpt_contract_snapshot")
public class ContractSnapshot {

    private String snapshotTime;

    private String contractId;

    private Long userId;

    private Long accountId;

    private BigDecimal contractBalance;

    private BigDecimal unrealizedPnl;

    private Date createdAt;
}
