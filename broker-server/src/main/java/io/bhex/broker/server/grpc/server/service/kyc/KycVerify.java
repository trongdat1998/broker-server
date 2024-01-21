package io.bhex.broker.server.grpc.server.service.kyc;

import io.bhex.broker.server.domain.KycLevelGrade;

public interface KycVerify<R, P> {

    int getLevel();

    KycLevelGrade getGrade();

    P verify(R request);
}
