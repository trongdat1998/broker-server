package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.TwoStepAuth;
import io.bhex.broker.grpc.payment.*;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.PaymentService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class PaymentGrpcService extends PaymentServiceGrpc.PaymentServiceImplBase {

    @Resource
    private AccountService accountService;

    @Resource
    private PaymentService paymentService;

    @Override
    public void createOrderWithPayeeAccountType(CreatePaymentOrderWithPayeeAccountTypeRequest request, StreamObserver<CreatePaymentOrderResponse> observer) {
        try {
            Long payeeUserId = accountService.getUserIdByAccountType(request.getHeader().getOrgId(), request.getPayeeAccountType());
            CreatePaymentOrderResponse response = paymentService.createOrder(request.getHeader(),
                    request.getClientOrderId(), request.getPayerUserId(), payeeUserId, request.getPayInfoList(), request.getProductInfoList(),
                    request.getEffectiveTime(), request.getDesc(), request.getExtendInfo());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("create payment order error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            CreatePaymentOrderResponse response = CreatePaymentOrderResponse.newBuilder().setRet(BrokerErrorCode.ORDER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void createOrder(CreatePaymentOrderRequest request, StreamObserver<CreatePaymentOrderResponse> observer) {
        try {
            CreatePaymentOrderResponse response = paymentService.createOrder(request.getHeader(), request.getClientOrderId(),
                    request.getPayerUserId(), request.getPayeeUserId(), request.getPayInfoList(), request.getProductInfoList(),
                    request.getEffectiveTime(), request.getDesc(), request.getExtendInfo());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("create payment order error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            CreatePaymentOrderResponse response = CreatePaymentOrderResponse.newBuilder().setRet(BrokerErrorCode.ORDER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void queryOrders(QueryPaymentOrderRequest request, StreamObserver<QueryPaymentOrderResponse> observer) {
        try {
            QueryPaymentOrderResponse response = paymentService.queryOrders(request.getHeader(), request.getPayerUserId(), request.getPayeeUserId(),
                    request.getOrderStatusList(), request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query payment orders error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void getOrderDetail(GetPaymentOrderDetailRequest request, StreamObserver<GetPaymentOrderDetailResponse> observer) {
        try {
            GetPaymentOrderDetailResponse response = paymentService.getPaymentOrder(request.getHeader(), request.getOrderId(), request.getClientOrderId(),
                    request.getPayerUserId(), request.getPayeeUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("get payment order detail error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void checkOrderPay(CheckOrderPayRequest request, StreamObserver<CheckOrderPayResponse> observer) {
        CheckOrderPayResponse response;
        try {
            response = paymentService.checkOrderPay(request.getHeader(), request.getOrderId());
        } catch (Exception e) {
            log.error("check payment order need2fa error, set need2FA = true, request:{}", JsonUtil.defaultGson().toJson(request), e);
            response = CheckOrderPayResponse.newBuilder().setNeed2Fa(true).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void orderPay(OrderPayRequest request, StreamObserver<OrderPayResponse> observer) {
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            OrderPayResponse response = paymentService.orderPay(request.getHeader(), request.getOrderId(), request.getPayeeUserId(), request.getCheck2Fa(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("pay payment order error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            OrderPayResponse response = OrderPayResponse.newBuilder().setRet(BrokerErrorCode.PAYMENT_PAY_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void orderSettle(OrderSettleRequest request, StreamObserver<OrderSettleResponse> observer) {
        try {
            OrderSettleResponse response = paymentService.orderSettle(request.getHeader(), request.getOrderId(), request.getClientOrderId(),
                    request.getPayeeUserId(), request.getTokenId(), request.getAmount(), request.getSettleClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("settle payment order error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void orderRefund(OrderRefundRequest request, StreamObserver<OrderRefundResponse> observer) {
        try {
            OrderRefundResponse response = paymentService.orderRefund(request.getHeader(), request.getOrderId(), request.getClientOrderId(),
                    request.getPayeeUserId(), request.getItemId(), request.getTokenId(), request.getAmount(), request.getRefundClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("refund payment order error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void orderCancel(OrderCancelRequest request, StreamObserver<OrderCancelResponse> observer) {
        try {
            OrderCancelResponse response = paymentService.orderCancel(request.getHeader(), request.getOrderId(), request.getClientOrderId(),
                    request.getPayeeUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancel payment orders error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void orderUnlock(OrderUnlockRequest request, StreamObserver<OrderUnlockResponse> observer) {
        try {
            OrderUnlockResponse response = paymentService.orderUnlock(request.getHeader(), request.getOrderId(), request.getClientOrderId(),
                    request.getPayeeUserId(), request.getTokenId(), request.getAmount(), request.getUnlockClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("unlock payment order error, request:{}", JsonUtil.defaultGson().toJson(request), e);
            observer.onError(e);
        }
    }
}
