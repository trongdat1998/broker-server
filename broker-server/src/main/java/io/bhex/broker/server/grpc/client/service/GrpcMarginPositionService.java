package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.margin.*;
import io.bhex.base.margin.LoanRequest;
import io.bhex.base.margin.cross.*;
import io.bhex.base.proto.ErrorCode;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-06-10 18:16
 */
@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcMarginPositionService extends GrpcBaseService {


    public LoanReply loan(LoanRequest request){
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceGrpc.MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            LoanReply reply = stub.loan(request);
            if (reply.getRet() != 0) {
                log.error("loan error clientOrderId:{} ret {}",request.getClientId(), reply.getRet());
                throw new BrokerException(BrokerErrorCode.MARGIN_LOAN_FAILED);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.MARGIN_LOAN_FAILED);
        }
    }

    public RepayResponse repayByLoanId(RepayRequest request){
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            RepayResponse reply = stub.repay(request);
            if (reply.getCode() != ErrorCode.SUCCESS) {
                log.error("repayByLoanId error loanId:{} code:{}  ",request.getLoanOrderId(), reply.getCode());
                throw new BrokerException(BrokerErrorCode.MARGIN_REPAY_FAILED);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.MARGIN_REPAY_FAILED);
        }
    }
    public GetAvailWithdrawAmountReply getAvailWithdrawAmount(GetAvailWithdrawAmountRequest request){
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceGrpc.MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            GetAvailWithdrawAmountReply reply = stub.getAvailWithdrawAmount(request);
            if (reply.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.MARGIN_GET_WITHDRAW_FAILED);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CrossLoanPositionReply getCrossLoanPosition(CrossLoanPositionRequest request){
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);

        try {
            CrossLoanPositionReply reply = stub.getCrossLoanPosition(request);
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetLoanAccountPositionReply getLoanAccountPosition(GetLoanAccountPositionRequest request){
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceGrpc.MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            GetLoanAccountPositionReply reply = stub.getLoanAccountPosition(request);
            if (reply.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.MARGIN_GET_ALL_POSITION_FAILED);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BatchRepayResponse batchRepay(BatchRepayRequest request){
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            BatchRepayResponse reply = stub.batchRepay(request);
            if (reply.getCode() != ErrorCode.SUCCESS) {
                throw new BrokerException(BrokerErrorCode.MARGIN_REPAY_FAILED);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
    public CalculateSafeByAccountReply calculateSafeByAccount(CalculateSafeByAccountReqeust request){
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceGrpc.MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            CalculateSafeByAccountReply reply = stub.calculateSafeByAccount(request);
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
