package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;

@Data
public class SSOLoginRequest {
    private String appId;
    private String version;
    private String nonce;
    private String userId;
    private String sign;
}
