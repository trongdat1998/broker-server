package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.ex.otc.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 订单消息（聊天记录）grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-17
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcOrderMessageGrpcService extends OTCMessageServiceGrpc.OTCMessageServiceImplBase {

    @Autowired
    private GrpcOtcService grpcOtcService;

    @Override
    public void addMessage(OTCNewMessageRequest request, StreamObserver<OTCNewMessageResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.sendMessage(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getMessages(OTCGetMessagesRequest request, StreamObserver<OTCGetMessagesResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getMessageList(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAppealMessages(GetAppealMessagesRequest request, StreamObserver<GetAppealMessagesResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getAppealMessageList(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }
}
