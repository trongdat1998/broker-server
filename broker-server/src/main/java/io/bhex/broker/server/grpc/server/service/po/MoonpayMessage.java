package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

@Data
public class MoonpayMessage {
    MoonpayTransaction data;
    String type;
    String externalCustomerId;
}