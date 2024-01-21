package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class IDCardOcrResponse extends WebankBaseResponse {

    private IDCardOcrObject result;
}
