package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class QueryResultResponse extends WebankBaseResponse {

    private FaceCompareObject result;
}
