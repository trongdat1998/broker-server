package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GetOAuthCodeResponse {

    private String oauthCode;
    private Long expired;
    private String state;
    private String redirectUrl;

}
