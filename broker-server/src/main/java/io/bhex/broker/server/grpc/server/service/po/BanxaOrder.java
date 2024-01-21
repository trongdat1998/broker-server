package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class BanxaOrder {
    private String id;
    private String account_id;
    private String account_reference;
    private String order_type;
    private String fiat_code;
    private BigDecimal fiat_amount;
    private String coin_code;
    private BigDecimal coin_amount;
    private String wallet_address;
    private String created_at;
    private String checkout_url;
    private String status;
    private Long payment_id;
    private String payment_code;
    private String payment_type;
    private String ref;
    private String fee;
    private String payment_fee;
    private Long commission;
    private String tx_hash;
}

