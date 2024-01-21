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
public class StatisticsWithdrawOrder {

    private Long orderId;
    private String clientWithdrawalId;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private String tokenId;
    private String address;
    private String addressTag;
    private BigDecimal totalQuantity;
    private BigDecimal arriveQuantity;
//    private BigDecimal minerFee;
//    private String minerFeeTokenId;
//    private BigDecimal platformFee;
//    private BigDecimal brokerFee;
//    private String feeTokenId;
    private String txId;
    private String txIdUrl;
    private Integer status;
    private Timestamp createdAt;
    private Timestamp updatedAt;
    //    private Integer isConvert;
    //    private BigDecimal brokerConvertPaid;
    //    private String language;
    //    private Integer withdrawMethod;
    private Long walletHandleTime;
//    private String extensionData;

}
