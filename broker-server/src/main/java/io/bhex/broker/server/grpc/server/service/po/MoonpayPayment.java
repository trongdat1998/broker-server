package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class MoonpayPayment {
    private Long id;
    private String type;
    private String name;
}

