package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.margin.*;
import io.bhex.broker.grpc.news.*;
import io.bhex.broker.server.grpc.server.service.MarginService;
import io.bhex.broker.server.grpc.server.service.NewsService;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;


@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class NewsGrpcService extends NewsServiceGrpc.NewsServiceImplBase {

    @Resource
    NewsService newsService;

    public void queryNews(QueryNewsRequest request, StreamObserver<QueryNewsResponse> responseObserver) {
        try {
            QueryNewsResponse response = newsService.queryNews(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.warn("queryNews "+e.getMessage(), e);
            QueryNewsResponse response = QueryNewsResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryNews "+e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getNews(GetNewsRequest request, StreamObserver<GetNewsResponse> responseObserver) {
        try {
            GetNewsResponse response = newsService.getNews(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.warn("getNews "+e.getMessage(), e);
            GetNewsResponse response = GetNewsResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryNews "+e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    public void createNews(CreateNewsRequest request, StreamObserver<CreateNewsResponse> responseObserver) {
        try {
            CreateNewsResponse response = newsService.createNews(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.warn("createNews "+e.getMessage(), e);
            CreateNewsResponse response = CreateNewsResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("createNews "+e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void modifyNews(ModifyNewsRequest request, StreamObserver<ModifyNewsResponse> responseObserver) {
        try {
            ModifyNewsResponse response = newsService.modifyNews(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.warn("modifyNews "+e.getMessage(), e);
            ModifyNewsResponse response = ModifyNewsResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("modifyNews "+e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    public void updateStatus(UpdateStatusRequest request, StreamObserver<UpdateStatusResponse> responseObserver) {
        try {
            UpdateStatusResponse response = newsService.updateStatus(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.warn("updateStatus "+e.getMessage(), e);
            UpdateStatusResponse response = UpdateStatusResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("updateStatus "+e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getTemplate(GetTemplateRequest request, StreamObserver<GetTemplateResponse> responseObserver) {
        try {
            GetTemplateResponse response = newsService.getTemplate(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.warn("getTemplate "+e.getMessage(), e);
            GetTemplateResponse response = GetTemplateResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getTemplate "+e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setTemplate(SetTemplateRequest request, StreamObserver<SetTemplateResponse> responseObserver) {
        try {
            SetTemplateResponse response = newsService.setTemplate(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.warn("setTemplate "+e.getMessage(), e);
            SetTemplateResponse response = SetTemplateResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setTemplate "+e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}
