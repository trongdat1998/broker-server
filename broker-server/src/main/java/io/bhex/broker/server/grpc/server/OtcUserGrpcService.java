package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.ex.otc.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 用户信息grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-19
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcUserGrpcService extends OTCUserServiceGrpc.OTCUserServiceImplBase {

    @Autowired
    private GrpcOtcService grpcOtcService;

    @Override
    public void setNickName(OTCSetNickNameRequest request, StreamObserver<OTCSetNickNameResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.setNickName(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getNickName(OTCGetNickNameRequest request, StreamObserver<OTCGetNickNameResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getUserInfo(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getUserContact(OTCGetNickNameRequest request,
                               StreamObserver<OTCUserContactResponse> responseObserver){

        try {
            responseObserver.onNext(grpcOtcService.getUserContact(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }

    }
}
