package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;

@Data
public class WebankBaseResponse {
    private String code;
    private String msg;
    private String transactionTime;
}
