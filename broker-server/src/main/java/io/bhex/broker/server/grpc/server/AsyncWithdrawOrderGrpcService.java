/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/24
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import io.bhex.base.account.AsyncWithdrawOrderRequest;
import io.bhex.base.account.AsyncWithdrawOrderResponse;
import io.bhex.base.account.AsyncWithdrawOrderServiceGrpc;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.grpc.server.service.WithdrawService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@GrpcService(interceptors = GrpcServerLogInterceptor.class)
@Slf4j
public class AsyncWithdrawOrderGrpcService extends AsyncWithdrawOrderServiceGrpc.AsyncWithdrawOrderServiceImplBase {

    @Resource
    private WithdrawService withdrawService;

    @Override
    public void asyncWithdrawOrder(AsyncWithdrawOrderRequest request, StreamObserver<AsyncWithdrawOrderResponse> observer) {
        AsyncWithdrawOrderResponse response;
        try {
            response = withdrawService.asyncWithdrawOrder(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AsyncWithdrawOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("async withdraw order status error", e);
            observer.onError(e);
        }
    }
}
