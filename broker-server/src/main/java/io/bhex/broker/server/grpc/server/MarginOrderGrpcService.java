package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.margin.MarginOrderServiceGrpc;
import io.bhex.broker.grpc.order.CreateOrderRequest;
import io.bhex.broker.grpc.order.CreateOrderResponse;
import io.bhex.broker.grpc.order.CreatePlanSpotOrderRequest;
import io.bhex.broker.grpc.order.CreatePlanSpotOrderResponse;
import io.bhex.broker.server.grpc.server.service.MarginOrderService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-12-23 10:29
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class MarginOrderGrpcService extends MarginOrderServiceGrpc.MarginOrderServiceImplBase {
    @Resource
    MarginOrderService marginOrderService;

    @Override
    public void marginRiskCreateOrder(CreateOrderRequest request, StreamObserver<CreateOrderResponse> responseObserver) {
        CreateOrderResponse response;
        try {
            BaseResult<CreateOrderResponse> baseResult = marginOrderService.createMarginOrderRisk(request);
            if (baseResult.isSuccess()) {
                response = baseResult.getData();
            } else {
                response = CreateOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = CreateOrderResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("margin create order error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void marginRiskCreatePlanSpotOrder(CreatePlanSpotOrderRequest request, StreamObserver<CreatePlanSpotOrderResponse> responseObserver) {
        CreatePlanSpotOrderResponse response;
        try {
            BaseResult<CreatePlanSpotOrderResponse> baseResult = marginOrderService.createMarginPlanSpotOrder(request);
            if (baseResult.isSuccess()) {
                response = baseResult.getData();
            } else {
                response = CreatePlanSpotOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = CreatePlanSpotOrderResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("margin create plan order error", e);
            responseObserver.onError(e);
        }
    }
}
