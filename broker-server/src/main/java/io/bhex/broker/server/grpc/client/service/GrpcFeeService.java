package io.bhex.broker.server.grpc.client.service;


import io.bhex.base.account.*;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@GrpcLog
@PrometheusMetrics
public class GrpcFeeService extends GrpcBaseService {

    public GetBrokerTradeMinFeeResponse getBrokerTradeMinFee(GetBrokerTradeMinFeeRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        FeeServiceGrpc.FeeServiceBlockingStub stub = grpcClientConfig.feeServiceBlockingStub(orgId);
        try {
            return stub.getBrokerTradeMinFee(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
