package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

@Data
public class BanxaPrice {
    private String payment_method_id;
    private String type;
    private String spot_price_fee;
    private String spot_price_including_fee;
    private String coin_amount;
    private String coin_code;
    private String fiat_amount;
    private String fiat_code;
    private String fee_amount;
}

