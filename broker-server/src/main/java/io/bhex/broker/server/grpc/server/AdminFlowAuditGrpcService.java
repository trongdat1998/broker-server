package io.bhex.broker.server.grpc.server;

import com.google.protobuf.TextFormat;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.auditflow.*;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.grpc.server.service.auditflow.FlowAuditLogService;
import io.bhex.broker.server.grpc.server.service.auditflow.FlowAuditService;
import io.bhex.broker.server.grpc.server.service.auditflow.FlowBizRecordService;
import io.bhex.broker.server.grpc.server.service.auditflow.FlowConfigService;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductAdminService;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductOrderService;
import io.bhex.broker.server.model.staking.StakingProductRebate;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

/**
 * flow audit grpc service
 *
 * @author songxd
 * @date 2021-01-15
 */
@Slf4j
@GrpcService
public class AdminFlowAuditGrpcService extends AdminAuditFlowServiceGrpc.AdminAuditFlowServiceImplBase {

    @Resource
    private FlowConfigService flowConfigService;

    @Resource
    private FlowBizRecordService flowBizRecordService;

    @Resource
    private FlowAuditLogService flowAuditLogService;

    @Resource
    private FlowAuditService flowAuditService;

    @Override
    public void saveFlowConfig(AdminSaveFlowConfigRequest request, StreamObserver<AdminSaveFlowConfigReply> responseObserver) {
        try {
            AdminSaveFlowConfigReply reply = flowConfigService.saveFlowConfig(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.saveFlowConfig with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setFlowForbidden(AdminSetFlowForbiddenRequest request, StreamObserver<AdminSetFlowForbiddenReply> responseObserver) {
        try {
            AdminSetFlowForbiddenReply reply = flowConfigService.setFlowForbidden(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.setFlowForbidden with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getFlowConfigList(AdminGetFlowConfigListRequest request, StreamObserver<AdminGetFlowConfigListReply> responseObserver) {
        try {
            AdminGetFlowConfigListReply reply = flowConfigService.getFlowConfigList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.getFlowConfigList with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getFlowBizTypeList(AdminGetFlowBizTypeListRequest request, StreamObserver<AdminGetFlowBizTypeListReply> responseObserver) {
        try {
            AdminGetFlowBizTypeListReply reply = flowConfigService.getFlowBizTypeList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.getFlowBizTypeList with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAuditRecordList(AdminFlowGetRecordListRequest request, StreamObserver<AdminFlowGetAuditRecordListReply> responseObserver) {
        try {
            AdminFlowGetAuditRecordListReply reply = flowBizRecordService.getAuditRecordList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.getAuditRecordList with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getApprovedRecordList(AdminFlowGetRecordListRequest request, StreamObserver<AdminFlowGetApprovedRecordListReply> responseObserver) {

        try {
            AdminFlowGetApprovedRecordListReply reply = flowBizRecordService.getApprovedList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.getApprovedRecordList with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAuditLogList(AdminFlowGetAuditLogListRequest request, StreamObserver<AdminFlowGetAuditLogListReply> responseObserver) {
        try {
            AdminFlowGetAuditLogListReply reply = flowAuditLogService.getAuditLogList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.getAuditLogList with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void auditProcess(AdminFlowAuditRequest request, StreamObserver<AdminFlowAuditReply> responseObserver) {

        try {
            AdminFlowAuditReply reply = flowAuditService.auditProcess(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.auditProcess with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getFlowConfigDetail(AdminGetFlowConfigDetailRequest request, StreamObserver<AdminGetFlowConfigDetailReply> responseObserver) {
        try {
            AdminGetFlowConfigDetailReply reply = flowConfigService.getFlowConfigDetail(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("AdminFlowAuditGrpcService.getFlowConfigDetail with request:[{}] error", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }
}
