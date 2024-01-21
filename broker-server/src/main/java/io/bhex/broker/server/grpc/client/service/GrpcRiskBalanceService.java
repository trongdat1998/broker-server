package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.account.*;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcRiskBalanceService extends GrpcBaseService {

    public AddRiskInTransferReply addRiskInTransfer(AddRiskInTransferRequest request) {
        RiskBalanceServiceGrpc.RiskBalanceServiceBlockingStub stub = grpcClientConfig.riskBalanceServiceBlockingStub(request.getOrgId());
        try {
            return stub.addRiskInTransfer(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ReleaseRiskInTransferReply releaseRiskInTransfer(ReleaseRiskInTransferRequest request) {
        RiskBalanceServiceGrpc.RiskBalanceServiceBlockingStub stub = grpcClientConfig.riskBalanceServiceBlockingStub(request.getOrgId());
        try {
            return stub.releaseRiskInTransfer(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
