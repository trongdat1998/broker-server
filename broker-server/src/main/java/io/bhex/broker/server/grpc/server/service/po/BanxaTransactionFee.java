package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.util.List;

@Data
public class BanxaTransactionFee {

    private String fiat_code;

    private String coin_code;

    private List<BanxaFee> fees;
}
