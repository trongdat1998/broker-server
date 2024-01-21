package io.bhex.broker.server.domain.kyc.tencent;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class GetApiTicketRequest {

    @JsonProperty("app_id")
    private String appId;

    @JsonProperty("access_token")
    private String accessToken;

    private String type;

    private String version;

    @JsonProperty("user_id")
    private String userId;
}
