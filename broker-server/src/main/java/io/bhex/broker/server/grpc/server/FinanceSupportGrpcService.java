package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.finance_support.*;
import io.bhex.broker.server.grpc.server.service.FinanceSupportService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class FinanceSupportGrpcService extends FinanceSupportServiceGrpc.FinanceSupportServiceImplBase {

    @Resource
    private FinanceSupportService financeSupportService;

    @Override
    public void createFinanceAccount(CreateFinanceAccountRequest request, StreamObserver<CreateFinanceAccountResponse> observer) {
        try {
            CreateFinanceAccountResponse response = financeSupportService.createFinanceAccount(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            CreateFinanceAccountResponse response = CreateFinanceAccountResponse.newBuilder().setRet(e.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Throwable e) {
            log.error("createFinanceAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryFinanceAccounts(QueryFinanceAccountsRequest request, StreamObserver<QueryFinanceAccountsResponse> observer) {
        try {
            QueryFinanceAccountsResponse response = financeSupportService.queryFinanceAccounts(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            QueryFinanceAccountsResponse response = QueryFinanceAccountsResponse.newBuilder().setRet(e.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Throwable e) {
            log.error("queryFinanceAccounts error", e);
            observer.onError(e);
        }
    }

    @Override
    public void financeAccountTransfer(FinanceAccountTransferRequest request, StreamObserver<FinanceAccountTransferResponse> observer) {
        try {
            FinanceAccountTransferResponse response = financeSupportService.financeAccountTransfer(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            FinanceAccountTransferResponse response = FinanceAccountTransferResponse.newBuilder().setRet(e.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Throwable e) {
            log.error("financeAccountTransfer error", e);
            observer.onError(e);
        }
    }
}
