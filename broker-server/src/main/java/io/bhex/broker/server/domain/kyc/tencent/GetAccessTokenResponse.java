package io.bhex.broker.server.domain.kyc.tencent;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class GetAccessTokenResponse extends WebankBaseResponse {

    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("expire_in")
    private String expireIn;

    @JsonProperty("expire_time")
    private String expireTime;
}
