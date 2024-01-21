package io.bhex.broker.server.domain.kyc.tencent;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TicketObject {

    private String value;

    @JsonProperty("expire_in")
    private String expireIn;

    @JsonProperty("expire_time")
    private String expireTime;
}
