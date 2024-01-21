package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.org_api.*;
import io.bhex.broker.server.grpc.server.service.OrgApiKeyService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OrgApiKeyGrpcService extends OrgApiKeyServiceGrpc.OrgApiKeyServiceImplBase {

    @Resource
    private OrgApiKeyService orgApiKeyService;

    @Override
    public void createOrgApiKey(CreateOrgApiKeyRequest request, StreamObserver<CreateOrgApiKeyResponse> observer) {
        CreateOrgApiKeyResponse response = null;
        try {
            response = orgApiKeyService.createOrgApiKey(request.getHeader(), request.getTag(), request.getType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateOrgApiKeyResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void updateOrgApiKeyIps(UpdateOrgApiKeyIpsRequest request, StreamObserver<UpdateOrgApiKeyResponse> observer) {
        UpdateOrgApiKeyResponse response = null;
        try {
            response = orgApiKeyService.updateApiKeyIps(request.getHeader(), request.getApiKeyId(), request.getIpWhiteList());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateOrgApiKeyResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void updateOrgApiKeyStatus(UpdateOrgApiKeyStatusRequest request, StreamObserver<UpdateOrgApiKeyResponse> observer) {
        UpdateOrgApiKeyResponse response = null;
        try {
            response = orgApiKeyService.updateApiKeyStatus(request.getHeader(), request.getApiKeyId(), request.getStatus());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateOrgApiKeyResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void deleteOrgApiKey(DeleteOrgApiKeyRequest request, StreamObserver<DeleteOrgApiKeyResponse> observer) {
        DeleteOrgApiKeyResponse response;
        try {
            response = orgApiKeyService.deleteApiKey(request.getHeader(), request.getApiKeyId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DeleteOrgApiKeyResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryOrgApiKeys(QueryOrgApiKeyRequest request, StreamObserver<QueryOrgApiKeyResponse> observer) {
        QueryOrgApiKeyResponse response;
        try {
            response = orgApiKeyService.queryApiKeys(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryOrgApiKeyResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }
}
