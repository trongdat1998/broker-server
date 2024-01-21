package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;

@Data
public class SDKLoginPrepareResponse {

    private String webankAppId;
    private String version;
    private String nonce;
    private String userId;
    private String sign;
    private String license;
    private String orderNo;
}
