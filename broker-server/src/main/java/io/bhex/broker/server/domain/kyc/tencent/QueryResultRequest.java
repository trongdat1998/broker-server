package io.bhex.broker.server.domain.kyc.tencent;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class QueryResultRequest {

    @JsonProperty("app_id")
    private String appId;

    private String version;

    private String nonce;

    @JsonProperty("order_no")
    private String orderNo;

    private String sign;

    @JsonProperty("get_file")
    private String getFile;
}
