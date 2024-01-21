/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.grpc.client
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.account.*;
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
public class GrpcDepositService extends GrpcBaseService {

    public AddressReply getAddress(GetAddressRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        DepositServiceGrpc.DepositServiceBlockingStub stub = grpcClientConfig.depositServiceBlockingStub(orgId);
        try {
            return stub.getAddress(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryAddressReply queryAddress(QueryAddressRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        DepositServiceGrpc.DepositServiceBlockingStub stub = grpcClientConfig.depositServiceBlockingStub(orgId);
        try {
            return stub.queryAddress(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @GrpcLog(printNoResponse = true)
    public DepositRecordList queryDepositOrder(GetDepositRecordsRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        DepositServiceGrpc.DepositServiceBlockingStub stub = grpcClientConfig.depositServiceBlockingStub(orgId);
        try {
            return stub.getDepositRecords(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ForAddressReply forAddress(ForAddressRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        DepositServiceGrpc.DepositServiceBlockingStub stub = grpcClientConfig.depositServiceBlockingStub(orgId);
        try {
            return stub.forAddress(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetDepositsForAdminReply getDepositsForAdmin(GetDepositsForAdminRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        DepositServiceGrpc.DepositServiceBlockingStub stub = grpcClientConfig.depositServiceBlockingStub(orgId);
        try {
            return stub.getDepositsForAdmin(request);
        } catch (StatusRuntimeException e) {
            log.error("getDepositsForAdmin:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ReceiptReply receipt(ReceiptRequest request) {
        Long orgId = request.getOrgId();
        DepositServiceGrpc.DepositServiceBlockingStub stub = grpcClientConfig.depositServiceBlockingStub(orgId);
        try {
            return stub.receipt(request);
        } catch (StatusRuntimeException e) {
            log.error("receipt:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
