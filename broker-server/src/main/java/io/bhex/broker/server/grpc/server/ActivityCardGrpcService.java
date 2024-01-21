package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.*;
import io.bhex.broker.server.grpc.server.service.activity.ActivityCardQueryService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class ActivityCardGrpcService extends BrokerActivityServiceGrpc.BrokerActivityServiceImplBase {

    @Resource
    private ActivityCardQueryService activityCardQueryService;

    @Override
    public void listWelfareCardLog(WelfardCardRequest request, StreamObserver<ListWelfareCardLogResponse> observer) {
        ListWelfareCardLogResponse response;
        try {
            response = activityCardQueryService.listWelfareCardLog(request);
            observer.onNext(response);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            response = ListWelfareCardLogResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    @Override
    public void listUserCard(CommonListRequest request, StreamObserver<ListUserCardResponse> observer) {
        ListUserCardResponse response;
        try {
            response = activityCardQueryService.listUserCard(request);
            observer.onNext(response);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            response = ListUserCardResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    @Override
    public void getUserCardAmount(CommonCardRequest request, StreamObserver<GetUserCardAmountResponse> observer) {
        GetUserCardAmountResponse response;
        try {
            response = activityCardQueryService.getUserCardAmount(request);
            observer.onNext(response);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            response = GetUserCardAmountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    @Override
    public void countUserCard(io.bhex.broker.grpc.activity.HeaderReqest request, StreamObserver<CountUserCardResponse> observer) {
        CountUserCardResponse response;
        try {
            response = activityCardQueryService.countUserCard(request);
            observer.onNext(response);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            response = CountUserCardResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        }
        observer.onCompleted();
    }
}
