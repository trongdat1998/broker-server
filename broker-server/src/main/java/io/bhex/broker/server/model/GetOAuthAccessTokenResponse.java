package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GetOAuthAccessTokenResponse {

    private String accessToken;
    private Long expired;
    private String openId;
    private Long userId;

}
