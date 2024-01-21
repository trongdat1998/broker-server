package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

@Data
public class PushResult {
    private String clientReqId;

    private Boolean result;

    private String msg;
}
