package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.server.grpc.server.service.ActivityLockInterestAdminService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService
public class AdminActivityGrpcService extends AdminActivityServiceGrpc.AdminActivityServiceImplBase {

    @Resource
    private ActivityLockInterestAdminService activityAdminService;

    @Override
    public void listActivity(ListActivityRequest request, StreamObserver<ListActivityReply> responseObserver) {
        try {
            ListActivityReply reply = activityAdminService.listActivity(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

    }

    @Override
    public void saveActivity(SaveActivityRequest request, StreamObserver<SaveActivityReply> responseObserver) {
        try {
            SaveActivityReply reply = activityAdminService.saveActivity(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void findActivity(FindActivityRequest request, StreamObserver<FindActivityReply> responseObserver) {
        try {
            FindActivityReply reply = activityAdminService.findProject(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void calculateActivityResult(CalculateActivityRequest request,
                                        StreamObserver<AdminCommonResponse> responseObserver) {
        try {
            AdminCommonResponse reply = activityAdminService.calculatePurchaseResult(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void confirmActivityResult(FindActivityRequest request,
                                      StreamObserver<AdminCommonResponse> responseObserver) {
        try {
            AdminCommonResponse reply = activityAdminService.confirmActivityResult(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void findActivityResult(FindActivityRequest request,
                                   StreamObserver<FindActivityResultReply> responseObserver) {

        try {
            FindActivityResultReply reply = activityAdminService.findActivityResult(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

    }

    @Override
    public void onlineActivity(OnlineRequest request, StreamObserver<AdminCommonResponse> responseObserver) {

        try {
            AdminCommonResponse reply = activityAdminService.onlineActivity(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

    }

    @Override
    public void listActivityOrder(FindActivityRequest request, StreamObserver<ListActivityOrderReply> responseObserver) {

        try {
            ListActivityOrderReply reply = activityAdminService.listActivityOrderProfile(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

    }

    @Override
    public void adminQueryAllActivityOrderInfo(AdminQueryAllActivityOrderInfoRequest request, StreamObserver<AdminQueryAllActivityOrderInfoReply> responseObserver) {
        try {
            AdminQueryAllActivityOrderInfoReply reply = activityAdminService.adminQueryAllActivityOrderInfo(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void saveIeoWhiteList(SaveIeoWhiteListRequest request, StreamObserver<SaveIeoWhiteListReply> responseObserver) {
        try {
            activityAdminService.saveIeoWhiteList(request);
            responseObserver.onNext(SaveIeoWhiteListReply.newBuilder().setCode(0).setMessage("").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("SaveIeoWhiteListReply brokerId {} projectId {} error {}", request.getBrokerId(), request.getProjectId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryIeoWhiteList(QueryIeoWhiteListRequest request, StreamObserver<QueryIeoWhiteListReply> responseObserver) {
        try {
            QueryIeoWhiteListReply reply = activityAdminService.queryIeoWhiteList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryIeoWhiteList brokerId {} projectId {} error {}", request.getBrokerId(), request.getProjectId(), e);
            responseObserver.onError(e);
        }
    }
}
