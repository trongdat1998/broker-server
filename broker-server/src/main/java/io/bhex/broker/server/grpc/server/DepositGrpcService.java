/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/24
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.deposit.*;
import io.bhex.broker.server.grpc.server.service.DepositService;
import io.grpc.stub.StreamObserver;

import javax.annotation.Resource;

@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class DepositGrpcService extends DepositServiceGrpc.DepositServiceImplBase {

    @Resource
    private DepositService depositService;

    @Override
    public void getDepositAddress(GetDepositAddressRequest request, StreamObserver<GetDepositAddressResponse> observer) {
        GetDepositAddressResponse response;
        try {
            response = depositService.getDepositAddress(request.getHeader(), request.getTokenId(), request.getChainType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetDepositAddressResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryUserDepositAddress(QueryUserDepositAddressRequest request, StreamObserver<QueryUserDepositAddressResponse> observer) {
        Header header = request.getHeader();
        QueryUserDepositAddressResponse response;
        try {
            response = depositService.queryDepositAddress(header.getOrgId(), header.getUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void getDepositOrderDetail(GetDepositOrderDetailRequest request, StreamObserver<GetDepositOrderDetailResponse> observer) {
        GetDepositOrderDetailResponse response;
        try {
            response = depositService.getDepositOrder(request.getHeader(), request.getOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetDepositOrderDetailResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryDepositOrders(QueryDepositOrdersRequest request, StreamObserver<QueryDepositOrdersResponse> observer) {
        QueryDepositOrdersResponse response;
        try {
            response = depositService.queryDepositOrder(request.getHeader(), request.getTokenId(),
                    request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryDepositOrdersResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void getUserInfoByAddress(GetUserInfoByAddressRequest request, StreamObserver<GetUserInfoByAddressResponse> observer) {
        GetUserInfoByAddressResponse response;
        try {
            response = depositService.getUserInfoByAddress(request.getHeader(), request.getTokenId(), request.getAddress(), request.getAddressExt(), request.getChainType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetUserInfoByAddressResponse.newBuilder().setAccountId(0L).setOrgId(0L).setUserId(0L).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }
}
