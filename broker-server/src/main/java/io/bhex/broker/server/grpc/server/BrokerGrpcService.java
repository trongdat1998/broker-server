/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/21
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.broker.BrokerServiceGrpc;
import io.bhex.broker.grpc.broker.QueryBrokerRequest;
import io.bhex.broker.grpc.broker.QueryBrokerResponse;
import io.bhex.broker.server.grpc.server.service.BrokerService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class BrokerGrpcService extends BrokerServiceGrpc.BrokerServiceImplBase {

    @Resource
    private BrokerService brokerService;

    @Override
    public void queryBrokers(QueryBrokerRequest request, StreamObserver<QueryBrokerResponse> observer) {
        QueryBrokerResponse response;
        try {
            response = brokerService.queryBrokers();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            log.error("query broker info error", e);
            response = QueryBrokerResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query broker info error", e);
            observer.onError(e);
        }
    }

}
