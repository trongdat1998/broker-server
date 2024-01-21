package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.grpc.admin.AdminBrokerDBToolsServiceGrpc;
import io.bhex.broker.grpc.admin.FetchOneRequest;
import io.bhex.broker.grpc.admin.FetchOneResponse;
import io.bhex.broker.server.grpc.server.service.AdminBrokerDBToolsService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService
public class AdminBrokerDBToolsGrpcService extends AdminBrokerDBToolsServiceGrpc.AdminBrokerDBToolsServiceImplBase {

    @Resource
    AdminBrokerDBToolsService adminBrokerDBToolsService;

    public void fetchOneBroker(FetchOneRequest request, StreamObserver<FetchOneResponse> responseObserver) {
        try {
            String result = adminBrokerDBToolsService.fetchOneBroker(request);
            responseObserver.onNext(FetchOneResponse.newBuilder().setResult(result).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onNext(FetchOneResponse.newBuilder().setResult(e.getMessage()).setRet(BrokerErrorCode.SYSTEM_ERROR.code()).build());
        }
    }

    public void fetchOneStatistics(FetchOneRequest request, StreamObserver<FetchOneResponse> responseObserver) {
        try {
            String result = adminBrokerDBToolsService.fetchOneStatistics(request);
            responseObserver.onNext(FetchOneResponse.newBuilder().setResult(result).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onNext(FetchOneResponse.newBuilder().setResult(e.getMessage()).setRet(BrokerErrorCode.SYSTEM_ERROR.code()).build());
        }
    }
}
