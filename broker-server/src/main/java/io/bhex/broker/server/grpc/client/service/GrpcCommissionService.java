package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.clear.BrokerUserFeeRequest;
import io.bhex.base.clear.BrokerUserFeeResponse;
import io.bhex.base.clear.CommissionServiceGrpc;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcCommissionService extends GrpcBaseService {

/*    public BrokerUserFeeResponse getUserFee(BrokerUserFeeRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        CommissionServiceGrpc.CommissionServiceBlockingStub stub = grpcClientConfig.commissionServiceBlockingStub(orgId);
        try {
            return stub.getBrokerUserFee(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
        throw commonStatusRuntimeException(e);
    }*/
}
