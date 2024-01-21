package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Timestamp;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class StatisticsBalance {

    private Long balanceId;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private String tokenId;
    private BigDecimal total;
    private BigDecimal available;
    private BigDecimal locked;
    private BigDecimal position;
    private Timestamp createdAt;
    private Timestamp updatedAt;

}
