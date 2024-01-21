package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.lamb.*;
import io.bhex.broker.server.grpc.server.service.ActivityLambService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 2019/5/29 11:40 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService
public class ActivityLambGrpcService extends ActivityLambServiceGrpc.ActivityLambServiceImplBase {

    private final ActivityLambService activityLambService;

    @Autowired
    public ActivityLambGrpcService(ActivityLambService activityLambService) {
        this.activityLambService = activityLambService;
    }

    @Override
    public void listProjectInfo(ListProjectInfoRequest request, StreamObserver<ListProjectInfoReply> responseObserver) {
        ListProjectInfoReply reply;
        try {
            reply = activityLambService.listProjectInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            reply = ListProjectInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void createLambOrder(CreateLambOrderRequest request, StreamObserver<CreateLambOrderReply> responseObserver) {
        CreateLambOrderReply reply;
        try {
            reply = activityLambService.createLambOrder(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            reply = CreateLambOrderReply.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void listOrderInfo(ListOrderInfoRequest request, StreamObserver<ListOrderInfoReply> responseObserver) {
        ListOrderInfoReply reply;
        try {
            reply = activityLambService.listOrderInfo(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            reply = ListOrderInfoReply.newBuilder().build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void settlementInterest(SettlementInterestRequest request, StreamObserver<SettlementInterestReply> responseObserver) {
        SettlementInterestReply reply;
        try {
            reply = activityLambService.settlementInterest(request);
            responseObserver.onNext(reply);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            reply = SettlementInterestReply.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }
}
