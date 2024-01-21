package io.bhex.broker.server.grpc.client.service;


import io.bhex.base.admin.common.*;
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
public class GrpcBrokerTradeFeeService extends GrpcBaseService {

    public GetMinTradeFeeRateReply getMinTradeFeeRate(GetMinTradeFeeRateRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BrokerTradeFeeSettingServiceGrpc.BrokerTradeFeeSettingServiceBlockingStub stub = grpcClientConfig.tradeFeeSettingServiceBlockingStub(orgId);
        try {
            return stub.getMinTradeFeeRate(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BrokerTradeFeeRateReply getLatestBrokerTradeFee(GetLatestBrokerTradeFeeRequest request) {
        BrokerTradeFeeSettingServiceGrpc.BrokerTradeFeeSettingServiceBlockingStub stub = grpcClientConfig.tradeFeeSettingServiceBlockingStub(request.getBrokerId());
        try {
            return stub.getLatestBrokerTradeFee(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
