package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.grpc.statistics.*;
import io.bhex.broker.server.grpc.server.service.OdsService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OdsGrpcService extends OdsServiceGrpc.OdsServiceImplBase {

    @Resource
    private OdsService odsService;

    @Override
    public void queryOdsData(QueryOdsDataRequest request, StreamObserver<QueryOdsDataResponse> observer) {
        observer.onNext(odsService.queryOdsData(request));
        observer.onCompleted();
    }

    @Override
    public void queryOdsTokenData(QueryOdsDataRequest request, StreamObserver<QueryOdsTokenDataResponse> observer) {
        observer.onNext(odsService.queryOdsTokenData(request));
        observer.onCompleted();
    }

    @Override
    public void queryOdsSymbolData(QueryOdsDataRequest request, StreamObserver<QueryOdsSymbolDataResponse> observer) {
        observer.onNext(odsService.queryOdsSymbolData(request));
        observer.onCompleted();
    }

    @Override
    public void queryTopData(QueryTopDataRequest request, StreamObserver<QueryTopDataResponse> observer) {
        observer.onNext(odsService.queryTopData(request));
        observer.onCompleted();
    }
}
