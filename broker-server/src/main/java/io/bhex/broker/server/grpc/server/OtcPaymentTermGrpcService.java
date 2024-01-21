package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.ex.otc.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 付款方式grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-19
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcPaymentTermGrpcService extends OTCPaymentTermServiceGrpc.OTCPaymentTermServiceImplBase {

    @Autowired
    private GrpcOtcService grpcOtcService;

    @Override
    public void addPaymentTerm(OTCNewPaymentRequest request, StreamObserver<OTCNewPaymentResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.addPaymentTerm(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getPaymentTerms(OTCGetPaymentRequest request, StreamObserver<OTCGetPaymentResponse> responseObserver) {

        try {

            responseObserver.onNext(grpcOtcService.getPaymentTerms(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void updatePaymentTerm(OTCUpdatePaymentRequest request,
                                  StreamObserver<OTCUpdatePaymentResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.updatePaymentTerm(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void configPaymentTerm(OTCConfigPaymentRequest request,
                                  StreamObserver<OTCConfigPaymentResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.configPaymentTerm(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void deletePaymentTerm(OTCDeletePaymentRequest request, StreamObserver<OTCDeletePaymentResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.deletePaymentTerm(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void switchPaymentVisible(SwitchPaymentVisibleRequest request, StreamObserver<SwitchPaymentVisibleResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.switchPaymentVisible(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getBrokerPaymentConfig(GetBrokerPaymentConfigRequest request, StreamObserver<GetBrokerPaymentConfigResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.getBrokerPaymentConfig(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }


    @Override
    public void addNewPaymentTerm(OTCCreateNewPaymentRequest request, StreamObserver<OTCCreateNewPaymentResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.addNewPaymentTerm(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void updateNewPaymentTerm(OTCUpdateNewPaymentRequest request,
                                  StreamObserver<OTCUpdateNewPaymentResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.updateNewPaymentTerm(request));
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

}
