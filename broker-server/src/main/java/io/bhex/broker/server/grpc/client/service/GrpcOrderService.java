/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.grpc.client
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import com.google.protobuf.TextFormat;
import io.bhex.base.account.*;
import io.bhex.base.exception.ErrorStatusRuntimeException;
import io.bhex.base.proto.ErrorCode;
import io.bhex.base.proto.ErrorStatus;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcOrderService extends GrpcBaseService {

    public BaseResult<NewOrderReply> createOrder(NewOrderRequest request) {
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(request.getOrgId());
        try {
            return BaseResult.success(stub.newOrder(request));
        } catch (StatusRuntimeException e) {
            return handleCreateOrderStatusRuntimeException(request, false, e);
        }
    }

    public BaseResult<NewOrderReply> handleCreateOrderStatusRuntimeException(NewOrderRequest request, boolean isAsync, StatusRuntimeException e) {
        ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
        if (errorStatus != null) {
            ErrorCode error = errorStatus.getCode();
            log.warn("{} failed: {}, reason:{}, order -> accountId:{} symbol:{} cid:{} \n (request: {})",
                    isAsync ? "asyncNewOrder" : "newOrder", error.name(), errorStatus.getReason(),
                    request.getAccountId(), request.getSymbolId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            if (error == ErrorCode.ERR_Unknown_Account) {
                return BaseResult.fail(BrokerErrorCode.ACCOUNT_NOT_EXIST, errorStatus);
            } else if (error == ErrorCode.ERR_Unknown_Token) {
                return BaseResult.fail(BrokerErrorCode.TOKEN_NOT_FOUND, errorStatus);
            } else if (error == ErrorCode.ERR_Insufficient_Balance) {
                return BaseResult.fail(BrokerErrorCode.INSUFFICIENT_BALANCE, errorStatus);
            } else if (error == ErrorCode.ERR_Duplicated_Order) {
                return BaseResult.fail(BrokerErrorCode.DUPLICATED_ORDER, errorStatus);
            } else if (error == ErrorCode.ERR_Invalid_Argument) {
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID, errorStatus);
            } else if (error == ErrorCode.ERR_Position_Max_Limited) {
                return BaseResult.fail(BrokerErrorCode.CREATE_ORDER_ERROR_REACH_HOLD_LIMIT, errorStatus);
            } else {
                return BaseResult.fail(BrokerErrorCode.ORDER_FAILED, errorStatus);
            }
        }
        log.error("{} failed: {} order -> accountId:{} symbol:{} cid:{} \n (request: {})", isAsync ? "asyncNewOrder" : "newOrder", e.getStatus().getCode(),
                request.getAccountId(), request.getSymbolId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
        if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
            return BaseResult.fail(BrokerErrorCode.CREATE_ORDER_TIMEOUT, "DEADLINE_EXCEEDED");
        }
        return BaseResult.fail(BrokerErrorCode.ORDER_FAILED, "Unknown reason");
    }

    public BaseResult<CancelOrderReply> cancelOrder(CancelOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            CancelOrderReply reply = stub.cancelOrder(request);
            if (reply.getCode() != ErrorCode.SUCCESS) {
                return handleCancelOrderException(request, reply.getCode(), false, null);
            }
            return BaseResult.success(reply);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                return handleCancelOrderException(request, error, false, errorStatus);
            }
            log.error("cancelOrder failed: {}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                    e.getStatus().getCode(), request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_TIMEOUT, "DEADLINE_EXCEEDED");
            }
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FAILED, "Unknown reason");
        }
    }

    public BaseResult<FastCancelOrderReply> fastCancelOrder(FastCancelOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return BaseResult.success(stub.fastCancelOrder(request));
        } catch (StatusRuntimeException e) {
            log.error("fastCancelOrder failed: {}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                    e.getStatus().getCode(), request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_TIMEOUT, "DEADLINE_EXCEEDED");
            }
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FAILED, "Unknown reason");
        }
    }

    public BaseResult<CancelOrderReply> handleCancelOrderException(CancelOrderRequest request, ErrorCode errorCode, boolean isAsync, ErrorStatus errorStatus) {
        log.warn("{} failed: {}, reason:{}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                isAsync ? "asyncCancelOrder" : "cancelOrder", errorCode.name(), errorStatus == null ? "" : errorStatus.getReason(),
                request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
        if (errorCode == ErrorCode.ERR_Unknown_Account) {
            return BaseResult.fail(BrokerErrorCode.ACCOUNT_NOT_EXIST, errorStatus);
        } else if (errorCode == ErrorCode.ERR_Order_Not_Found) {
            return BaseResult.fail(BrokerErrorCode.ORDER_NOT_FOUND, errorStatus);
        } else if (errorCode == ErrorCode.ERR_Order_Filled) {
            return BaseResult.fail(BrokerErrorCode.ORDER_HAS_BEEN_FILLED, errorStatus);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_CANCELLED) {
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_CANCELLED, errorStatus);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_FINISHED) {
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FINISHED, errorStatus);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_REJECTED) {
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_REJECTED, errorStatus);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_NOT_FOUND) {
            return BaseResult.fail(BrokerErrorCode.ORDER_NOT_FOUND, errorStatus);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_ORDER_BE_LOCKED) {
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_LOCKED, errorStatus);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_UNSUPPORTED_ORDERTYPE) {
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_UNSUPPORTED_ORDER_TYPE, errorStatus);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_MAYBE_ARCHIVED) {
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_ARCHIVED, errorStatus);
        } else if (errorCode == ErrorCode.ORD_4015) {
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_ARCHIVED, errorStatus);
        } else {
            log.error("{} failed: {}, reason:{}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                    isAsync ? "asyncCancelOrder" : "cancelOrder", errorCode.name(), errorStatus == null ? "" : errorStatus.getReason(),
                    request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FAILED, errorStatus);
        }
    }

    public BatchCancelOrderReply batchCancelOrder(BatchCancelOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.batchCancelOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.error("batchCancelOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
            }
            log.error("{}", printStatusRuntimeException(e));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_TIMEOUT);
            }
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
        }
    }

    public CancelMatchOrderReply orgBatchCancelOrder(CancelMatchOrderRequest request) {
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(request.getBrokerId());
        try {
            return stub.cancelBrokerOrders(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.error("orgBatchCancelOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
            }
            log.error("{} orgBatchCancelOrder {} failed, {}", request.getBrokerId(), request.getSymbolId(), printStatusRuntimeException(e));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_TIMEOUT);
            }
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
        }
    }

    @GrpcLog(printNoResponse = true)
    public GetOrdersReply queryOrders(GetOrdersRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.shortDeadlineOrderServiceBlockingStub(orgId);
        try {
            return stub.getOrders(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOrderReply getOrderInfo(GetOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                if (error == ErrorCode.ERR_Unknown_Account) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                } else if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @GrpcLog(printNoResponse = true)
    public GetTradesReply queryMatchInfo(GetTradesRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.shortDeadlineOrderServiceBlockingStub(orgId);
        try {
            return stub.getTrades(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOrderTradeDetailReply getMatchInfo(GetOrderTradeDetailRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getOrderTradeDetail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @GrpcLog(printNoResponse = true)
    public GetBestOrderResponse getBestOrder(GetBestOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getBestOrder(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @GrpcLog(printNoResponse = true)
    public GetDepthInfoResponse getDepthInfo(GetDepthInfoRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        FuturesServerGrpc.FuturesServerBlockingStub stub = grpcClientConfig.futuresServerStub(orgId);
        try {
            return stub.getDepthInfo(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }


    public BaseResult<NewPlanSpotOrderReply> createPlanSpotOrder(NewPlanSpotOrderRequest request) {
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(request.getBaseRequest().getOrganizationId());
        try {
            return BaseResult.success(stub.newPlanSpotOrder(request));
        } catch (StatusRuntimeException e) {
            return handleCreatePlanSpotOrderStatusRuntimeException(request, false, e);
        }
    }

    public GetPlanSpotOrderReply getPlanSpotOrder(GetPlanSpotOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getPlanSpotOrder(request);
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

    @GrpcLog(printNoResponse = true)
    public GetPlanSpotOrdersReply getPlanSpotOrders(GetPlanSpotOrdersRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getPlanSpotOrders(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseResult<NewPlanSpotOrderReply> handleCreatePlanSpotOrderStatusRuntimeException(NewPlanSpotOrderRequest request, boolean isAsync, StatusRuntimeException e) {
        ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
        if (errorStatus != null) {
            ErrorCode error = errorStatus.getCode();
            log.warn("{} failed: {}, reason:{}, order -> accountId:{} symbol:{} cid:{} \n (request: {})",
                    isAsync ? "asyncNewPlanSpotOrder" : "newPlanSpotOrder", error.name(), errorStatus.getReason(),
                    request.getAccountId(), request.getSymbolId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            if (error == ErrorCode.ERR_Duplicated_Order) {
                return BaseResult.fail(BrokerErrorCode.DUPLICATED_ORDER, errorStatus);
            } else if (error == ErrorCode.ERR_Invalid_Argument) {
                return BaseResult.fail(BrokerErrorCode.PARAM_INVALID, errorStatus);
            } else if (error == ErrorCode.ORD_4017) {
                return BaseResult.fail(BrokerErrorCode.ORDER_PLAN_NEW_LIMIT, errorStatus);
            } else {
                return BaseResult.fail(BrokerErrorCode.ORDER_FAILED, errorStatus);
            }
        }
        log.error("{} failed: {} order -> accountId:{} symbol:{} cid:{} \n (request: {})", isAsync ? "asyncNewPlanSpotOrder" : "newPlanSpotOrder", e.getStatus().getCode(),
                request.getAccountId(), request.getSymbolId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
        if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
            return BaseResult.fail(BrokerErrorCode.CREATE_ORDER_TIMEOUT, "DEADLINE_EXCEEDED");
        }
        return BaseResult.fail(BrokerErrorCode.ORDER_FAILED, "Unknown reason");
    }

    public BaseResult<CancelPlanSpotOrderReply> cancelPlanSpotOrder(CancelPlanSpotOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            CancelPlanSpotOrderReply reply = stub.cancelPlanSpotOrder(request);
            if (reply.getCode() != ErrorCode.SUCCESS) {
                return handleCancelPlanSpotOrderException(request, reply.getCode(), false, null);
            }
            return BaseResult.success(reply);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                return handleCancelPlanSpotOrderException(request, error, false, errorStatus);
            }
            log.error("cancelPlanSpotOrder failed: {}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                    e.getStatus().getCode(), request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_TIMEOUT, "DEADLINE_EXCEEDED");
            }
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FAILED, "Unknown reason");
        }
    }

    public BaseResult<CancelPlanSpotOrderReply> handleCancelPlanSpotOrderException(CancelPlanSpotOrderRequest request, ErrorCode errorCode, boolean isAsync, ErrorStatus errorStatus) {
        log.warn("{} failed: {}, reason:{}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                isAsync ? "asyncCancelPlanSpotOrder" : "cancelPlanSpotOrder", errorCode.name(), errorStatus == null ? "" : errorStatus.getReason(),
                request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
        if (errorCode == ErrorCode.ERR_Order_Not_Found) {
            return BaseResult.fail(BrokerErrorCode.ORDER_NOT_FOUND, errorStatus);
        } else {
            log.error("{} failed: {}, reason:{}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                    isAsync ? "asyncCancelPlanSpotOrder" : "cancelPlanSpotOrder", errorCode.name(), errorStatus == null ? "" : errorStatus.getReason(),
                    request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            return BaseResult.fail(BrokerErrorCode.CANCEL_ORDER_FAILED, errorStatus);
        }
    }

    public BatchCancelPlanSpotOrderReply batchCancelPlanSpotOrder(BatchCancelPlanSpotOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.batchCancelPlanSpotOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.error("batchCancelPlanSpotOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
            }
            log.error("{}", printStatusRuntimeException(e));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_TIMEOUT);
            }
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
        }
    }

}
