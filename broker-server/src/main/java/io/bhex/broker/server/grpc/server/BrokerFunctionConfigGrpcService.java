package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.function.config.BrokerFunctionConfigServiceGrpc;
import io.bhex.broker.grpc.function.config.GetBrokerFunctionConfigRequest;
import io.bhex.broker.grpc.function.config.GetBrokerFunctionConfigResponse;
import io.bhex.broker.grpc.function.config.SetBrokerFunctionConfigRequest;
import io.bhex.broker.grpc.function.config.SetBrokerFunctionConfigResponse;
import io.bhex.broker.server.grpc.server.service.BrokerFunctionConfigService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class BrokerFunctionConfigGrpcService extends BrokerFunctionConfigServiceGrpc.BrokerFunctionConfigServiceImplBase {

    @Autowired
    private BrokerFunctionConfigService brokerFunctionConfigService;


    public void getBrokerFunctionConfig(GetBrokerFunctionConfigRequest request,
                                        StreamObserver<GetBrokerFunctionConfigResponse> observer) {
        GetBrokerFunctionConfigResponse response;
        try {
            response = brokerFunctionConfigService.getBrokerFunctionConfigList(request.getHeader().getOrgId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerFunctionConfigResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void getAllBrokerFunctionConfig(GetBrokerFunctionConfigRequest request,
                                           StreamObserver<GetBrokerFunctionConfigResponse> responseObserver) {
        try {
            GetBrokerFunctionConfigResponse response = brokerFunctionConfigService.getALLBrokerFunctionConfigList(
                request.getHeader() != null ? request.getHeader().getOrgId() : null,
                request.getFunction() != null ? request.getFunction().name() : null
            );
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void setBrokerFunctionConfig(SetBrokerFunctionConfigRequest request,
                                        StreamObserver<SetBrokerFunctionConfigResponse> responseObserver) {
        try {
            brokerFunctionConfigService.setBrokerFunctionConfig(request.getHeader().getOrgId(),
                request.getFunction().name(), request.getStatus());
            responseObserver.onNext(SetBrokerFunctionConfigResponse.newBuilder().setRet(0).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
