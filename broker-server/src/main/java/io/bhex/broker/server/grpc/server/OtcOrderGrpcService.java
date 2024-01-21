package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.grpc.server.service.OtcService;
import io.bhex.ex.otc.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

/**
 * 订单grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-16
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcOrderGrpcService extends OTCOrderServiceGrpc.OTCOrderServiceImplBase {

    @Autowired
    private GrpcOtcService grpcOtcService;

    @Autowired
    private OtcService otcService;

    @Resource(name = "userRequestHandleTaskExecutor")
    private TaskExecutor userRequestHandleTaskExecutor;

    @Override
    public void addOrder(OTCNewOrderRequest request, StreamObserver<OTCNewOrderResponse> responseObserver) {
        try {
            responseObserver.onNext(otcService.createOrder(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void handleOrder(OTCHandleOrderRequest request, StreamObserver<OTCHandleOrderResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.handleOrder(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

    }

    @Override
    public void getOrders(OTCGetOrdersRequest request, StreamObserver<OTCGetOrdersResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getOrderList(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOrderByFromId(OTCGetOrdersRequest request, StreamObserver<OTCGetOrdersResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.getOrderByFromId(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOrderInfo(OTCGetOrderInfoRequest request, StreamObserver<OTCGetOrderInfoResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getOrder(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getPendingOrders(OTCGetPendingOrdersRequest request,
                                 StreamObserver<OTCGetPendingOrdersResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.getPendingOrders(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getPendingOrderCount(OTCGetPendingCountRequest request,
                                     StreamObserver<OTCGetPendingCountResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            try {
                responseObserver.onNext(grpcOtcService.getPendingOrderCount(request));
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                responseObserver.onError(e);
            }
        }, userRequestHandleTaskExecutor);
    }

    @Override
    public void getNewOrderInfo(OTCGetOrderInfoRequest request, StreamObserver<OTCGetOrderInfoResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.getNewOrder(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getLastNewOrderId(OTCGetPendingCountRequest request,
                                  StreamObserver<GetLastNewOrderIdResponse> responseObserver){

        try {
            responseObserver.onNext(grpcOtcService.getLastNewOrderId(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}
