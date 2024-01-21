/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.grpc.client
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bhex.base.account.*;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.NoGrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcWithdrawService extends GrpcBaseService {

    public CheckWithdrawalAddressResponse checkAddress(CheckWithdrawalAddressRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        WithdrawalServiceGrpc.WithdrawalServiceBlockingStub stub = grpcClientConfig.withdrawalServiceBlockingStub(orgId);
        try {
            return stub.checkWithdrawalAddress(request);
        } catch (StatusRuntimeException e) {
            log.error("checkAddress:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public WithdrawalQuotaResponse queryWithdrawQuota(WithdrawalQuotaRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        WithdrawalServiceGrpc.WithdrawalServiceBlockingStub stub = grpcClientConfig.withdrawalServiceBlockingStub(orgId);
        try {
            return stub.withdrawalQuota(request);
//            WithdrawalQuotaResponse response = stub.withdrawalQuota(request);
//            if (response.getResult() == WithdrawalResult.FORBIDDEN) {
//                throw new BrokerException(BrokerErrorCode.WITHDRAW_NOT_ALLOW);
//            }
//            return response;
        } catch (StatusRuntimeException e) {
            log.error("queryWithdrawQuota:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public WithdrawalResponse withdraw(WithdrawalRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        WithdrawalServiceGrpc.WithdrawalServiceBlockingStub stub = grpcClientConfig.withdrawalServiceBlockingStub(orgId);
        try {
            WithdrawalResponse response = stub.withdrawal(request);
            WithdrawalResult result = response.getWithdrawalResult();
            if (result != WithdrawalResult.WITHDRAWAL_RESULT_SUCCESS) {
                if (result == WithdrawalResult.CONFIRMATION_CODE_EXPIRED || result == WithdrawalResult.CONFIRMATION_CODE_INCORRECT) {
                    if (request.getConfirmationCodeType() == ConfirmationCodeType.MOBILE) {
                        throw new BrokerException(BrokerErrorCode.VERIFY_CODE_ERROR);
                    }
                    throw new BrokerException(BrokerErrorCode.EMAIL_VERIFY_CODE_ERROR);
                } else if (result == WithdrawalResult.INSUFFICIENT_BALANCE) {
                    throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
                } else if (result == WithdrawalResult.FORBIDDEN) {
                    throw new BrokerException(BrokerErrorCode.WITHDRAW_NOT_ALLOW);
                }
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("withdraw:{}", printStatusRuntimeException(e));
            try {
                log.error("withdraw:[{}] error, {}", JsonUtil.defaultProtobufJsonPrinter().print(request), printStatusRuntimeException(e));
            } catch (InvalidProtocolBufferException ex) {
                // ignore
            }
            throw commonStatusRuntimeException(e);
        }
    }

    public CancelWithdrawResponse cancelWithdrawOrder(CancelWithdrawRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        WithdrawalServiceGrpc.WithdrawalServiceBlockingStub stub = grpcClientConfig.withdrawalServiceBlockingStub(orgId);
        try {
            return stub.cancelWithdraw(request);
        } catch (StatusRuntimeException e) {
            log.error("cancelWithdrawOrder:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @GrpcLog(printNoResponse = true)
    public GetWithdrawalOrderResponse queryWithdrawOrders(GetWithdrawalOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        WithdrawalServiceGrpc.WithdrawalServiceBlockingStub stub = grpcClientConfig.withdrawalServiceBlockingStub(orgId);
        try {
            return stub.getWithdrawalOrder(request);
        } catch (StatusRuntimeException e) {
            log.error("queryWithdrawOrders:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @NoGrpcLog
    public GetFeeTokenIdResponse getFeeTokenId(GetFeeTokenIdRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        WithdrawalServiceGrpc.WithdrawalServiceBlockingStub stub = grpcClientConfig.withdrawalServiceBlockingStub(orgId);
        try {
            return stub.getFeeTokenId(request);
        } catch (StatusRuntimeException e) {
            log.error("getFeeTokenId:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetWithdrawalAuditStatusResponse setWithdrawalAuditStatus(SetWithdrawalAuditStatusRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        WithdrawalServiceGrpc.WithdrawalServiceBlockingStub stub = grpcClientConfig.withdrawalServiceBlockingStub(orgId);
        try {
            return stub.setWithdrawalAuditStatus(request);
        } catch (StatusRuntimeException e) {
            log.error("setWithdrawalAuditStatus:{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

}
