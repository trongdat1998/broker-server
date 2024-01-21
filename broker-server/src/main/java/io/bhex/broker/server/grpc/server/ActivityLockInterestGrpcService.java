package io.bhex.broker.server.grpc.server;

import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.lockInterest.ActivityLockInterestServiceGrpc;
import io.bhex.broker.grpc.activity.lockInterest.ActivityOrderTaskToFailRequest;
import io.bhex.broker.grpc.activity.lockInterest.ActivityOrderTaskToFailResponse;
import io.bhex.broker.grpc.activity.lockInterest.CreateActivityOrderTaskRequest;
import io.bhex.broker.grpc.activity.lockInterest.CreateActivityOrderTaskResponse;
import io.bhex.broker.grpc.activity.lockInterest.CreateLockInterestOrderReply;
import io.bhex.broker.grpc.activity.lockInterest.CreateLockInterestOrderRequest;
import io.bhex.broker.grpc.activity.lockInterest.ExecuteActivityOrderTaskRequest;
import io.bhex.broker.grpc.activity.lockInterest.ExecuteActivityOrderTaskResponse;
import io.bhex.broker.grpc.activity.lockInterest.ListActivityReply;
import io.bhex.broker.grpc.activity.lockInterest.ListActivityRequest;
import io.bhex.broker.grpc.activity.lockInterest.ListLockOrderInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.ListLockOrderInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.ListLockProjectInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.ListLockProjectInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.ModifyActivityOrderInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.ModifyActivityOrderInfoResponse;
import io.bhex.broker.grpc.activity.lockInterest.OrgListActivityReply;
import io.bhex.broker.grpc.activity.lockInterest.OrgListActivityRequest;
import io.bhex.broker.grpc.activity.lockInterest.OrgQueryActivityOrderInfoByUserRequest;
import io.bhex.broker.grpc.activity.lockInterest.OrgQueryActivityOrderInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.OrgQueryActivityOrderInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.ProjectCommonInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.ProjectCommonInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryActivityProjectInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryActivityProjectInfoResponse;
import io.bhex.broker.grpc.activity.lockInterest.QueryAllActivityOrderInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.QueryAllActivityOrderInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryMyPerformanceRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryMyPerformanceResponse;
import io.bhex.broker.grpc.activity.lockInterest.QueryTradeCompetitionInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.QueryTradeCompetitionInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.SignUpTradeCompetitionRequest;
import io.bhex.broker.grpc.activity.lockInterest.SignUpTradeCompetitionResponse;
import io.bhex.broker.server.grpc.server.service.ActivityLockInterestAdminService;
import io.bhex.broker.server.grpc.server.service.ActivityLockInterestService;
import io.bhex.broker.server.grpc.server.service.TradeCompetitionService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 2019/6/4 11:44 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService
public class ActivityLockInterestGrpcService extends ActivityLockInterestServiceGrpc.ActivityLockInterestServiceImplBase {

    @Autowired
    private ActivityLockInterestService lockInterestService;

    @Autowired
    private TradeCompetitionService tradeCompetitionService;

    @Resource
    private ActivityLockInterestAdminService activityLockInterestAdminService;

    @Override
    public void listProjectInfo(ListLockProjectInfoRequest request, StreamObserver<ListLockProjectInfoReply> responseObserver) {
        ListLockProjectInfoReply reply;
        try {
            reply = lockInterestService.listProjectInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = ListLockProjectInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void createLockInterestOrder(CreateLockInterestOrderRequest request, StreamObserver<CreateLockInterestOrderReply> responseObserver) {
        CreateLockInterestOrderReply reply;
        try {
            reply = lockInterestService.createOrder(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = CreateLockInterestOrderReply.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error("createLockInterestOrder failed {} ", e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void listOrderInfo(ListLockOrderInfoRequest request, StreamObserver<ListLockOrderInfoReply> responseObserver) {
        ListLockOrderInfoReply reply;
        try {
            reply = lockInterestService.listOrderInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = ListLockOrderInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void projectCommonInfo(ProjectCommonInfoRequest request, StreamObserver<ProjectCommonInfoReply> responseObserver) {
        ProjectCommonInfoReply reply;
        try {
            reply = lockInterestService.projectCommonInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = ProjectCommonInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void queryAllActivityOrderInfo(QueryAllActivityOrderInfoRequest request, StreamObserver<QueryAllActivityOrderInfoReply> responseObserver) {
        QueryAllActivityOrderInfoReply reply;
        try {
            reply = lockInterestService.queryAllActivityOrderInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = QueryAllActivityOrderInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void orgQueryActivityOrderInfo(OrgQueryActivityOrderInfoRequest request, StreamObserver<OrgQueryActivityOrderInfoReply> responseObserver) {
        OrgQueryActivityOrderInfoReply reply;
        try {
            reply = lockInterestService.orgQueryActivityOrderInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = OrgQueryActivityOrderInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void orgQueryActivityOrderInfoByUser(OrgQueryActivityOrderInfoByUserRequest request, StreamObserver<OrgQueryActivityOrderInfoReply> responseObserver) {
        OrgQueryActivityOrderInfoReply reply;
        try {
            reply = lockInterestService.orgQueryActivityOrderInfoByUser(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = OrgQueryActivityOrderInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void listActivity(ListActivityRequest request, StreamObserver<ListActivityReply> responseObserver) {

        ListActivityReply reply;
        try {
            reply = lockInterestService.listActivity(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = ListActivityReply.newBuilder().setMessage(e.getMessage()).build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void orgListActivity(OrgListActivityRequest request, StreamObserver<OrgListActivityReply> responseObserver) {

        OrgListActivityReply reply;
        try {
            reply = lockInterestService.orgListActivity(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = OrgListActivityReply.newBuilder().setMessage(e.getMessage()).build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void queryTradeCompetitionInfo(QueryTradeCompetitionInfoRequest request, StreamObserver<QueryTradeCompetitionInfoReply> responseObserver) {
        QueryTradeCompetitionInfoReply reply;
        try {
            reply = tradeCompetitionService.queryTradeCompetitionDetail(request.getHeader().getOrgId(),
                    request.getHeader().getUserId(),
                    request.getHeader().getLanguage(),
                    request.getCompetitionCode(),
                    request.getRankType());
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = QueryTradeCompetitionInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void queryMyPerformance(QueryMyPerformanceRequest request,
                                   StreamObserver<QueryMyPerformanceResponse> responseObserver) {

        QueryMyPerformanceResponse reply;
        try {
            reply = tradeCompetitionService.queryMyPerformance(request.getHeader().getOrgId(),
                    request.getHeader().getUserId(),
                    request.getCompetitionCode());
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = QueryMyPerformanceResponse.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();

    }

    //交易大赛参赛名单
    @Override
    public void signUpTradeCompetition(SignUpTradeCompetitionRequest request, StreamObserver<SignUpTradeCompetitionResponse> responseObserver) {
        SignUpTradeCompetitionResponse response;
        try {
            response = tradeCompetitionService.signUpTradeCompetition(request);
            responseObserver.onNext(response);
        } catch (BrokerException e) {
            response = SignUpTradeCompetitionResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void queryActivityProjectInfo(QueryActivityProjectInfoRequest request, StreamObserver<QueryActivityProjectInfoResponse> responseObserver) {
        QueryActivityProjectInfoResponse reply;
        try {
            reply = lockInterestService.queryActivityProjectInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = QueryActivityProjectInfoResponse.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void createActivityOrderTask(CreateActivityOrderTaskRequest request, StreamObserver<CreateActivityOrderTaskResponse> responseObserver) {
        CreateActivityOrderTaskResponse response;
        try {
            response = activityLockInterestAdminService.createActivityOrderTask(request);
            responseObserver.onNext(response);
        } catch (BrokerException e) {
            response = CreateActivityOrderTaskResponse.getDefaultInstance();
            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void executeActivityOrderTask(ExecuteActivityOrderTaskRequest request, StreamObserver<ExecuteActivityOrderTaskResponse> responseObserver) {
        ExecuteActivityOrderTaskResponse response;
        try {
            response = activityLockInterestAdminService.executeActivityOrderTask(request);
            responseObserver.onNext(response);
        } catch (BrokerException e) {
            response = ExecuteActivityOrderTaskResponse.getDefaultInstance();
            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void modifyActivityOrderInfo(ModifyActivityOrderInfoRequest request, StreamObserver<ModifyActivityOrderInfoResponse> responseObserver) {
        ModifyActivityOrderInfoResponse reply;
        try {
            reply = activityLockInterestAdminService.modifyActivityOrderInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            reply = ModifyActivityOrderInfoResponse.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void activityOrderTaskToFail(ActivityOrderTaskToFailRequest request, StreamObserver<ActivityOrderTaskToFailResponse> observer) {
        ActivityOrderTaskToFailResponse response;
        try {
            response = activityLockInterestAdminService.activityOrderTaskToFail(request);
            observer.onNext(response);
        } catch (BrokerException e) {
            response = ActivityOrderTaskToFailResponse.getDefaultInstance();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        }
        observer.onCompleted();
    }
}
