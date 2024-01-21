package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.admin.AdminOrgInfoServiceGrpc;
import io.bhex.base.admin.QueryBrokerOrgReplay;
import io.bhex.base.admin.QueryBrokerOrgRequest;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


/**
 * @author wangsc
 * @description
 * @date 2020-06-29 19:44
 */
@Slf4j
@Service
@GrpcLog
@PrometheusMetrics
public class GrpcAdminOrgInfoService extends GrpcBaseService {

    public QueryBrokerOrgReplay queryBrokerOrg(QueryBrokerOrgRequest request) {
        AdminOrgInfoServiceGrpc.AdminOrgInfoServiceBlockingStub stub = grpcClientConfig.orgInfoServiceBlockingStub(request.getBrokerId());
        try {
            return stub.queryBrokerOrg(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            return QueryBrokerOrgReplay.newBuilder().setRegisterOption(0).build();
        }
    }
}
