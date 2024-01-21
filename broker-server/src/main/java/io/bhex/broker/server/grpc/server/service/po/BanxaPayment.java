package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.util.List;

@Data
public class BanxaPayment {
    private Long id;
    private String paymentType;
    private String name;
    private String status;
    private List<String> supported_fiat;
    private List<String> supported_coin;
    private List<BanxaTransactionFee> transaction_fees;
    private List<BanxaTransactionLimits> transaction_limits;
}
