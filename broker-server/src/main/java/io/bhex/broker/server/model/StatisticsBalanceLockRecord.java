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
public class StatisticsBalanceLockRecord {

    private Long id;
    private Long recordId;
    private Long orgId;
    private Long clientReqId;
    private Long accountId;
    private String tokenId;
    private Integer businessSubject;
    private Integer secondBusinessSubject;
    private Long subjectExtId;
    private BigDecimal lockAmount;
    private Long balanceId;
    private BigDecimal newLockedValue;
    private String status;
    private BigDecimal unlockedValue;
    private Timestamp createdAt;
    private Timestamp updatedAt;
    private String lockReason;
    private String shardId;
    private Long userId;
}
