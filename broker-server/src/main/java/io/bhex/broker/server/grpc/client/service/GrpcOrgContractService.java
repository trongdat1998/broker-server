package io.bhex.broker.server.grpc.client.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bhex.base.admin.AdminOrgContractServiceGrpc;
import io.bhex.base.admin.ListAllContractByOrgIdsRequest;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.NoGrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.base.admin.ListContractReply;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.client.service
 * @Author: ming.xu
 * @CreateDate: 04/11/2018 5:42 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcOrgContractService extends GrpcBaseService {

    @NoGrpcLog
    public ListContractReply listAllContract(ListAllContractByOrgIdsRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        AdminOrgContractServiceGrpc.AdminOrgContractServiceBlockingStub stub = grpcClientConfig.orgContractServiceBlockingStub(orgId);
        try {
            return stub.listAllContractByOrgIds(request);
        } catch (StatusRuntimeException e) {
            try {
                log.error("listAllContract error, request:{}, {}", JsonUtil.defaultProtobufJsonPrinter().print(request), printStatusRuntimeException(e));
            } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
                invalidProtocolBufferException.printStackTrace();
            }
            throw commonStatusRuntimeException(e);
        }
    }
}
