package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.gateway.*;
import io.bhex.broker.server.grpc.interceptor.RouteAuthCredentials;
import io.bhex.broker.server.grpc.server.service.BrokerAuthService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.Set;

/**
 * @author wangsc
 * @description broker认证
 * @date 2020-06-06 17:28
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class BrokerAuthGrpcService extends BrokerAuthServiceGrpc.BrokerAuthServiceImplBase {

    @Resource
    BrokerAuthService brokerAuthService;

    @Override
    public void getBrokerAuthByOrgId(GetBrokerAuthByOrgIdRequest request, StreamObserver<GetBrokerAuthByOrgIdResponse> responseObserver) {
        GetBrokerAuthByOrgIdResponse response;
        try {
            RouteAuthCredentials.BrokerAuth brokerAuth = brokerAuthService.getBrokerAuthByOrgId(request.getHeader().getOrgId());
            if(brokerAuth == null){
                response = GetBrokerAuthByOrgIdResponse.newBuilder().setRet(-1001).build();
            }else{
                response = GetBrokerAuthByOrgIdResponse.newBuilder()
                        .setRet(0)
                        .setBrokerOrgAuth(GetBrokerAuthByOrgIdResponse.BrokerOrgAuth.newBuilder()
                                .setApiKey(brokerAuth.getApiKey())
                                .setAuthData(brokerAuth.getAuthData())
                                .setRefreshTime(brokerAuth.getRefreshTime())
                                .build())
                        .build();

            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerAuthByOrgIdResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("get broker auth by org_id error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getALLAuthOrgIds(GetALLAuthOrgIdsRequest request, StreamObserver<GetALLAuthOrgIdsResponse> responseObserver) {
        GetALLAuthOrgIdsResponse response;
        try {
            Set<Long> orgIds = brokerAuthService.getALLAuthOrgIds();
            response = GetALLAuthOrgIdsResponse.newBuilder()
                    .addAllOrgIds(orgIds)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetALLAuthOrgIdsResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("get all auth org_ids error", e);
            responseObserver.onError(e);
        }
    }
}
