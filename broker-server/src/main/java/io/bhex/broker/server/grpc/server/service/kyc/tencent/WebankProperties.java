package io.bhex.broker.server.grpc.server.service.kyc.tencent;

import lombok.Data;

@Data
public class WebankProperties {

    private String idascUrl = "https://idasc.webank.com";
    private String idaUrl = "https://ida.webank.com";
}
