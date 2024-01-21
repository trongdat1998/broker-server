package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class MoonpayPrice {

    private BigDecimal baseCurrencyAmount;
    private BigDecimal extraFeeAmount;
    private BigDecimal feeAmount;
    private BigDecimal networkFeeAmount;
    private BigDecimal quoteCurrencyAmount;
    private BigDecimal totalAmount;
    private String paymentMethod;

}

