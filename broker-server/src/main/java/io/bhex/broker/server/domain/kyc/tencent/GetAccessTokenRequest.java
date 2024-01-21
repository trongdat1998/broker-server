package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;

@Data
public class GetAccessTokenRequest {
    private String appId;
    private String secret;
    private String grantType;
    private String version;
}
