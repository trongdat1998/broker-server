package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.account.*;
import io.bhex.base.exception.ErrorStatusRuntimeException;
import io.bhex.base.proto.ErrorCode;
import io.bhex.base.proto.ErrorStatus;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.OtcBaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcPayService extends GrpcBaseService {

    public CreateOrderReply createPaymentOrder(CreateOrderRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.createOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Unknown_Account) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                } else if (error == ErrorCode.ERR_Insufficient_Balance) {
                    throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetPayOrdersReply queryPaymentOrders(GetPayOrdersRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.getPayOrders(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetPayOrderReply getPaymentOrder(GetPayOrderRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.getPayOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CheckQuickpassReply checkOrderPay(CheckQuickpassRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(orgId);
        try {
            return stub.checkQuickpass(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ConfirmPayReply orderPay(ConfirmPayRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.confirmPay(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Insufficient_Balance) {
                    throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
                } else if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                } else if (error == ErrorCode.Pay_Cannot_Quickpass) {
                    throw new BrokerException(BrokerErrorCode.PAYMENT_CANNOT_SKIP_2FA);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public LockedSettleReply orderSettle(LockedSettleRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.lockedSettle(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public RefundReply orderRefund(RefundRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.refund(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CancelPayOrderReply orderCancel(CancelPayOrderRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.cancelPayOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public UnlockReply orderUnlock(UnlockRequest request) {
        PayServiceGrpc.PayServiceBlockingStub stub = grpcClientConfig.payServiceBlockingStub(request.getOrgId());
        try {
            return stub.unlock(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
