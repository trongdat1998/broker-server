package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.util.List;

@Data
public class BanxaPayments {
    private List<BanxaPayment> payment_methods;
}
