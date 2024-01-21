package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.user.kyc.*;
import io.bhex.broker.server.grpc.server.service.UserKycService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class UserKycGrpcService extends UserKycServiceGrpc.UserKycServiceImplBase {

    @Resource
    private UserKycService userKycService;

    @Resource(name = "kycRequestHandleTaskExecutor")
    private TaskExecutor kycRequestHandleTaskExecutor;

    @Override
    public void getWebankFaceId(GetWebankFaceIdRequest request, StreamObserver<GetWebankFaceIdResponse> observer) {
        super.getWebankFaceId(request, observer);
    }

    @Override
    public void webankSdkLoginPrepare(WebankSdkLoginPrepareRequest request, StreamObserver<WebankSdkLoginPrepareResponse> observer) {
        CompletableFuture.runAsync(() -> {
            WebankSdkLoginPrepareResponse response;
            try {
                response = userKycService.webankSdkLoginPrepare(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = WebankSdkLoginPrepareResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("webankSdkLoginPrepare error", e);
                observer.onError(e);
            }
        }, kycRequestHandleTaskExecutor);
    }

    @Override
    public void basicKycVerify(BasicKycVerifyRequest request, StreamObserver<BasicKycVerifyResponse> observer) {
        BasicKycVerifyResponse response;
        try {
            response = userKycService.basicKycVerify(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BasicKycVerifyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("basicKycVerify error, request:[orgId:{} userId:{} name:{}]",
                    request.getHeader().getOrgId(), request.getHeader().getUserId(), request.getName(), e);
            observer.onError(e);
        }
    }

    @Override
    public void photoKycVerify(PhotoKycVerifyRequest request, StreamObserver<PhotoKycVerifyResponse> observer) {
        CompletableFuture.runAsync(() -> {
            PhotoKycVerifyResponse response;
            try {
                response = userKycService.photoKycVerify(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = PhotoKycVerifyResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("photoKycVerify error", e);
                observer.onError(e);
            }
        }, kycRequestHandleTaskExecutor);
    }

    @Override
    public void faceKycVerify(FaceKycVerifyRequest request, StreamObserver<FaceKycVerifyResponse> observer) {
        CompletableFuture.runAsync(() -> {
            FaceKycVerifyResponse response;
            try {
                response = userKycService.faceKycVerify(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = FaceKycVerifyResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("faceKycVerify error", e);
                observer.onError(e);
            }
        }, kycRequestHandleTaskExecutor);
    }

    @Override
    public void videoKycVerify(VideoKycVerifyRequest request, StreamObserver<VideoKycVerifyResponse> observer) {
        CompletableFuture.runAsync(() -> {
            VideoKycVerifyResponse response;
            try {
                response = userKycService.videoKycVerify(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = VideoKycVerifyResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("videoKycVerify error", e);
                observer.onError(e);
            }
        }, kycRequestHandleTaskExecutor);
    }

    @Override
    public void orgBatchUpdateKyc(OrgBatchUpdateKycRequest request, StreamObserver<OrgBatchUpdateKycResponse> observer) {
        CompletableFuture.runAsync(() -> {
            OrgBatchUpdateKycResponse response = userKycService.orgBatchUpdateKyc(request);
            try {
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = OrgBatchUpdateKycResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("orgBatchUpdateKyc error", e);
                observer.onError(e);
            }
        }, kycRequestHandleTaskExecutor);
    }

    @Override
    public void orgBatchUpdateSeniorKyc(OrgBatchUpdateSeniorKycRequest request, StreamObserver<OrgBatchUpdateSeniorKycResponse> observer) {
        CompletableFuture.runAsync(() -> {
            OrgBatchUpdateSeniorKycResponse response = userKycService.orgBatchUpdateSeniorKyc(request);
            try {
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = OrgBatchUpdateSeniorKycResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("orgBatchUpdateSeniorKyc error", e);
                observer.onError(e);
            }
        }, kycRequestHandleTaskExecutor);
    }
}
