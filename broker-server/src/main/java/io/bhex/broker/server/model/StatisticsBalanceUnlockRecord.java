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
public class StatisticsBalanceUnlockRecord {


    private Long id;

    private Long orgId;

    private Long clientReqId;

    private Long originClientReqId;

    private Long lockRecordId;

    private Long accountId;

    private String tokenId;

    private Integer businessSubject;

    private Integer secondBusinessSubject;

    private Long subjectExtId;

    private BigDecimal unlockAmount;

    private Long balanceId;

    private BigDecimal newLockedValue;

    private Timestamp createdAt;

    private String unlockReason;

    private String shardId;

    private Long userId;
}
