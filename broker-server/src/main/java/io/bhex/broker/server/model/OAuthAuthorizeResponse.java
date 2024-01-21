package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class OAuthAuthorizeResponse {

    private String requestId;
    private String appName;
    private List<ThirdPartyAppOpenFunction> functions;

}
