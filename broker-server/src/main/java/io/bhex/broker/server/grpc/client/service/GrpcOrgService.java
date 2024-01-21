package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.account.GetOrgByIdReply;
import io.bhex.base.account.GetOrgByIdRequest;
import io.bhex.base.account.OrgServiceGrpc;
import io.bhex.base.admin.AdminOrgContractServiceGrpc;
import io.bhex.base.admin.ListAllContractByOrgIdsRequest;
import io.bhex.base.admin.ListContractReply;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.NoGrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcOrgService  extends GrpcBaseService {
    public GetOrgByIdReply getOrgById(GetOrgByIdRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrgServiceGrpc.OrgServiceBlockingStub stub = grpcClientConfig.orgServiceStub(orgId);
        try {
            return stub.getOrgById(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
