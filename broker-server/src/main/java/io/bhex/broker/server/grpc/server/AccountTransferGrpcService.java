package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.transfer.UserAccountTransferGrpc;
import io.bhex.broker.grpc.transfer.UserAccountTransferRequest;
import io.bhex.broker.grpc.transfer.UserAccountTransferResponse;
import io.bhex.broker.server.grpc.server.service.AccountTransferService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 21/01/2019 8:20 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class AccountTransferGrpcService extends UserAccountTransferGrpc.UserAccountTransferImplBase {

    @Autowired
    AccountTransferService accountTransferService;

    @Override
    public void userAccountTransferCoinToOption(UserAccountTransferRequest request, StreamObserver<UserAccountTransferResponse> observer) {
        UserAccountTransferResponse response;
        try {
            response = accountTransferService.userAccountTransfer(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UserAccountTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
            log.error(" userAccountTransferCoinToOption exception:{}", request, e);
        } catch (Exception e) {
            log.error(" userAccountTransferCoinToOption exception:{}", request, e);
            observer.onError(e);
        }
    }

    @Override
    public void userAccountTransfer(UserAccountTransferRequest request, StreamObserver<UserAccountTransferResponse> observer) {
        UserAccountTransferResponse response;
        try {
            response = accountTransferService.userAccountTransfer(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UserAccountTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
            log.error(" userAccountTransfer exception:{}", request, e);
        } catch (Exception e) {
            log.error(" userAccountTransfer exception:{}", request, e);
            observer.onError(e);
        }
    }
}
