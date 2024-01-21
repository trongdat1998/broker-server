package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

@Data
public class BanxaTransactionLimits {
    private String fiat_code;

    private String min;

    private String max;
}
