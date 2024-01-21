package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class MoonpayCurrency {

    private String id;
    private String type;
    private String name;
    private String code;
    private Integer precision;
    private BigDecimal minAmount;
    private BigDecimal maxAmount;
    private String createdAt;
    private String updatedAt;

}
