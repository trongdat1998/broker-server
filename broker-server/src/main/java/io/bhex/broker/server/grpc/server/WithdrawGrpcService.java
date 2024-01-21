/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/24
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.TwoStepAuth;
import io.bhex.broker.grpc.withdraw.*;
import io.bhex.broker.server.grpc.server.service.WithdrawService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@GrpcService(interceptors = GrpcServerLogInterceptor.class)
@Slf4j
public class WithdrawGrpcService extends WithdrawServiceGrpc.WithdrawServiceImplBase {

    @Resource
    private WithdrawService withdrawService;

    @Override
    public void queryAddresses(QueryAddressRequest request, StreamObserver<QueryAddressResponse> observer) {
        QueryAddressResponse response;
        try {
            response = withdrawService.queryAddress(request.getHeader(), request.getTokenId(), request.getChainType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryAddressResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query withdraw address list error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createAddress(CreateAddressRequest request, StreamObserver<CreateAddressResponse> observer) {
        CreateAddressResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = withdrawService.createAddress(request.getHeader(), request.getTokenId(), request.getChainType(),
                    request.getAddress(), request.getAddressExt(), request.getRemark(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateAddressResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("create withdraw address error", e);
            observer.onError(e);
        }
    }

    @Override
    public void checkAddress(CheckAddressRequest request, StreamObserver<CheckAddressResponse> observer) {
        CheckAddressResponse response;
        try {
            response = withdrawService.checkAddress(request.getHeader(), request.getTokenId(), request.getChainType(),
                    request.getAddress(), request.getAddressExt());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CheckAddressResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("create withdraw address error", e);
            observer.onError(e);
        }
    }

    @Override
    public void deleteAddresses(DeleteAddressRequest request, StreamObserver<DeleteAddressResponse> observer) {
        DeleteAddressResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = withdrawService.deleteAddress(request.getHeader(), request.getAddressId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DeleteAddressResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("delete withdraw address error", e);
            observer.onError(e);
        }
    }

    @Override
    public void withdrawQuota(WithdrawQuotaRequest request, StreamObserver<WithdrawQuotaResponse> observer) {
        WithdrawQuotaResponse response;
        try {
            response = withdrawService.withdrawQuota(request.getHeader(), request.getTokenId(), request.getChainType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = WithdrawQuotaResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("withdrawQuota error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void withdraw(WithdrawRequest request, StreamObserver<WithdrawResponse> observer) {
        WithdrawResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = withdrawService.withdraw(request.getHeader(), request.getTokenId(), request.getChainType(), request.getClientOrderId(), request.getAddressId(),
                    request.getAddress(), request.getAddressExt(), request.getWithdrawQuantity(), request.getMinerFee(),
                    request.getIsAutoConvert(), request.getConvertRate(), request.getTradePassword(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode(), request.getRemarks(),
                    request.getUserName(), request.getUserAddress());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = WithdrawResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("withdraw error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void orgWithdraw(OrgWithdrawRequest request, StreamObserver<OrgWithdrawResponse> observer) {
        OrgWithdrawResponse response;
        try {
            response = withdrawService.orgWithdraw(request.getHeader(), request.getTokenId(), request.getChainType(), request.getClientOrderId(),
                    request.getAddress(), request.getAddressExt(), request.getWithdrawQuantity(), request.getMinerFee(),
                    request.getIsAutoConvert(), request.getConvertRate(), request.getTradePassword(), request.getRemarks());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = OrgWithdrawResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("orgWithdraw error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void withdrawVerifyCode(WithdrawVerifyCodeRequest request, StreamObserver<WithdrawVerifyCodeResponse> observer) {
        WithdrawVerifyCodeResponse response;
        try {
            response = withdrawService.getWithdrawVerifyCode(request.getHeader(), request.getRequestId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = WithdrawVerifyCodeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("withdrawVerifyCode error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void withdrawVerify(WithdrawVerifyRequest request, StreamObserver<WithdrawVerifyResponse> observer) {
        WithdrawVerifyResponse response;
        try {
            response = withdrawService.withdrawVerify(request.getHeader(),
                    request.getRequestId(), request.getSkipCheckIdCardNo(), request.getIdCardNo(),
                    request.getCodeOrderId(), request.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = WithdrawVerifyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("withdrawVerify error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void getWithdrawOrderDetail(GetWithdrawOrderDetailRequest request, StreamObserver<GetWithdrawOrderDetailResponse> observer) {
        GetWithdrawOrderDetailResponse response;
        try {
            response = withdrawService.getWithdrawOrder(request.getHeader(), request.getOrderId(), request.getClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetWithdrawOrderDetailResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryWithdrawOrders(QueryWithdrawOrdersRequest request, StreamObserver<QueryWithdrawOrdersResponse> observer) {
        QueryWithdrawOrdersResponse response;
        try {
            response = withdrawService.queryWithdrawOrder(request.getHeader(), request.getTokenId(),
                    request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryWithdrawOrdersResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void openApiWithdraw(OpenApiWithdrawRequest request, StreamObserver<OpenApiWithdrawResponse> observer) {
        OpenApiWithdrawResponse response;
        try {
            response = withdrawService.openApiWithdraw(request.getHeader(), request.getClientOrderId(),
                    request.getAddress(), request.getAddressExt(), request.getTokenId(), request.getWithdrawQuantity(),
                    request.getChainType(), request.getIsAutoConvert(), request.getIsQuick());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = OpenApiWithdrawResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("openapi Withdraw error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void cancelWithdrawOrder(CancelWithdrawOrderRequest request, StreamObserver<CancelWithdrawOrderResponse> observer) {
        CancelWithdrawOrderResponse response;
        try {
            response = withdrawService.cancelWithdrawOrder(request.getHeader(), request.getOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CancelWithdrawOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("cancelWithdrawOrder error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void getBrokerWithdrawOrderInfo(GetBrokerWithdrawOrderInfoRequest request, StreamObserver<GetBrokerWithdrawOrderInfoResponse> observer) {
        GetBrokerWithdrawOrderInfoResponse response;
        try {
            response = withdrawService.getBrokerWithdrawOrderInfo(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerWithdrawOrderInfoResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getBrokerWithdrawOrderInfo error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getBrokerWithdrawOrderInfoList(GetBrokerWithdrawOrderInfoListRequest request, StreamObserver<GetBrokerWithdrawOrderInfoListResponse> observer) {
        GetBrokerWithdrawOrderInfoListResponse response;
        try {
            response = withdrawService.getBrokerWithdrawOrderInfoList(request.getHeader(), request.getTokenId(), request.getFromId(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerWithdrawOrderInfoListResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getBrokerWithdrawOrderInfoList error", e);
            observer.onError(e);
        }
    }


}
