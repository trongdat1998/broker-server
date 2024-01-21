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
public class StatisticsDepositOrder {

    private Long orderId;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private String tokenId;
    private BigDecimal quantity;
    private String fromAddress;
    private String walletAddress;
    private String walletAddressTag;
    private String txId;
    private String txIdUrl;
    private Integer status;
    private Timestamp createdAt;
    private Timestamp updatedAt;
    private Integer depositReceiptType;
    private Integer cannotReceiptReason;

}
