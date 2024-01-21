package io.bhex.broker.server.grpc.server;


import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.fee.*;
import io.bhex.broker.server.grpc.server.service.BrokerFeeService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class BrokerFeeGrpcService extends FeeServiceGrpc.FeeServiceImplBase {

    @Resource
    private BrokerFeeService brokerFeeService;

    @Override
    public void addDiscountFeeConfig(AddDiscountFeeConfigRequest request, StreamObserver<AddDiscountFeeConfigResponse> observer) {
        AddDiscountFeeConfigResponse response;
        try {
            String userStr = brokerFeeService.saveDiscountFeeConfig(request);
            response = AddDiscountFeeConfigResponse.newBuilder().setUserList(userStr).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AddDiscountFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("addDiscountFeeConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryDiscountFeeConfigList(QueryDiscountFeeConfigRequest request, StreamObserver<QueryDiscountFeeConfigResponse> observer) {
        QueryDiscountFeeConfigResponse response;
        try {
            response = brokerFeeService.queryDiscountFeeConfigList(request.getOrgId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryDiscountFeeConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryDiscountFeeConfigList error", e);
            observer.onError(e);
        }
    }

    @Override
    public void addSymbolFeeConfig(AddSymbolFeeConfigRequest request, StreamObserver<AddSymbolFeeConfigResponse> observer) {
        AddSymbolFeeConfigResponse response;
        try {
            response = brokerFeeService.saveSymbolFeeConfig(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("addSymbolFeeConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void querySymbolFeeConfigList(QuerySymbolFeeConfigRequest request, StreamObserver<QuerySymbolFeeConfigResponse> observer) {
        QuerySymbolFeeConfigResponse response;
        try {
            response = brokerFeeService.querySymbolFeeConfigList(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QuerySymbolFeeConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("querySymbolFeeConfigList error", e);
            observer.onError(e);
        }
    }

    @Override
    public void saveUserDiscountConfig(SaveUserDiscountConfigRequest request, StreamObserver<SaveUserDiscountConfigResponse> observer) {
        SaveUserDiscountConfigResponse response;
        try {
            brokerFeeService.saveDiscountUser(request.getOrgId(), request.getUserId(), request.getDiscountId(), request.getIsBase());
            response = SaveUserDiscountConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SaveUserDiscountConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("saveUserDiscountConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelUserDiscountConfig(CancelUserDiscountConfigRequest request, StreamObserver<CancelUserDiscountConfigResponse> observer) {
        CancelUserDiscountConfigResponse response;
        try {
            brokerFeeService.cancelUserLevel(request.getOrgId(), request.getUserId(), request.getIsBase());
            response = CancelUserDiscountConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CancelUserDiscountConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancelUserDiscountConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryOneDiscountFeeConfig(QueryOneDiscountFeeConfigRequest request, StreamObserver<QueryOneDiscountFeeConfigResponse> observer) {
        QueryOneDiscountFeeConfigResponse response;
        try {
            response = brokerFeeService.queryOneDiscountFeeConfigList(request.getDiscountId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryOneDiscountFeeConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryOneDiscountFeeConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryUserDiscountConfig(QueryUserDiscountConfigRequest request, StreamObserver<QueryUserDiscountConfigResponse> observer) {
        QueryUserDiscountConfigResponse response;
        try {
            response = brokerFeeService.queryFeeLevelUserByUserId(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryUserDiscountConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryUserDiscountConfig error", e);
            observer.onError(e);
        }
    }


    @Override
    public void saveSymbolMarketAccount(SaveSymbolMarketAccountRequest request, StreamObserver<SaveSymbolMarketAccountResponse> observer) {
        SaveSymbolMarketAccountResponse response;
        try {
            brokerFeeService.saveSymbolMarketAccountConfig(request.getSymbolFeeList());
            response = SaveSymbolMarketAccountResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = SaveSymbolMarketAccountResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("saveSymbolMarketAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAccountTradeFeeConfig(QueryAccountTradeFeeConfigRequest request, StreamObserver<QueryAccountTradeFeeConfigResponse> observer) {
        QueryAccountTradeFeeConfigResponse response;
        try {
            response = brokerFeeService.queryAccountTradeFeeConfig(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryAccountTradeFeeConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAccountTradeFeeConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void deleteAccountTradeFeeConfig(DeleteAccountTradeFeeConfigRequest request, StreamObserver<DeleteAccountTradeFeeConfigResponse> observer) {
        DeleteAccountTradeFeeConfigResponse response;
        try {
            brokerFeeService.deleteAccountTradeFeeConfig(request);
            response = DeleteAccountTradeFeeConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DeleteAccountTradeFeeConfigResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("deleteAccountTradeFeeConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAllSymbolMarketAccount(QueryAllSymbolMarketAccountRequest request, StreamObserver<QueryAllSymbolMarketAccountResponse> observer) {
        QueryAllSymbolMarketAccountResponse response;
        try {
            response = brokerFeeService.queryAllSymbolMarketAccount(request.getOrgId(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryAllSymbolMarketAccountResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAllSymbolMarketAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void deleteSymbolMarketAccount(DeleteSymbolMarketAccountRequest request, StreamObserver<DeleteSymbolMarketAccountResponse> observer) {
        DeleteSymbolMarketAccountResponse response;
        try {
            brokerFeeService.deleteSymbolMarketAccount(request.getId());
            observer.onNext(DeleteSymbolMarketAccountResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DeleteSymbolMarketAccountResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("DeleteSymbolMarketAccountResponse error", e);
            observer.onError(e);
        }
    }

    @Override
    public void bindUserDiscountConfig(io.bhex.broker.grpc.fee.BindUserDiscountConfigRequest request,
                                       io.grpc.stub.StreamObserver<io.bhex.broker.grpc.fee.BindUserDiscountConfigResponse> observer) {
        BindUserDiscountConfigResponse response;
        try {
            response = brokerFeeService.bindUserDiscountConfig(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BindUserDiscountConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("BindUserDiscountConfigResponse error", e);
            observer.onError(e);
        }
    }

    public void unbindUserDiscountConfig(io.bhex.broker.grpc.fee.UnbindUserDiscountConfigRequest request,
                                         io.grpc.stub.StreamObserver<io.bhex.broker.grpc.fee.UnbindUserDiscountConfigResponse> observer) {
        UnbindUserDiscountConfigResponse response;
        try {
            response = brokerFeeService.unbindUserDiscountConfig(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UnbindUserDiscountConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("UnbindUserDiscountConfigResponse error", e);
            observer.onError(e);
        }
    }
}
