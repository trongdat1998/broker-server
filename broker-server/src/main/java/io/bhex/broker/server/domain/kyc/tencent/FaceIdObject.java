package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;

@Data
public class FaceIdObject {
    private String bizSeqNo;
    private String orderNo;
    private String faceId;
}
