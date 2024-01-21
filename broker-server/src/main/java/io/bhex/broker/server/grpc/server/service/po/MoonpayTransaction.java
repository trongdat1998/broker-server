package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class MoonpayTransaction {
    private String id;
    private BigDecimal baseCurrencyAmount;
    private BigDecimal quoteCurrencyAmount;
    private BigDecimal extraFeeAmount;
    private BigDecimal feeAmount;
    private BigDecimal networkFeeAmount;
    private String status;
    private String failureReason;
    private String externalTransactionId;
    private String externalCustomerId;

}

