package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class GetFaceIdResponse extends WebankBaseResponse {
    private FaceIdObject result;
}
