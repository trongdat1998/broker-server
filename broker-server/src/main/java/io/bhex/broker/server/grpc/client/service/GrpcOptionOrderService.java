/*
 ************************************
 * @项目名称: broker-server
 * @文件名称: GrpcOptionOrderService
 * @Date 2019/01/09
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
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
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.MockUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcOptionOrderService extends GrpcBaseService {

    private static final boolean mock = false;

    public NewOrderReply createOptionOrder(NewOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return mock ? MockUtil.createOrder() : stub.newOptionOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.warn("newOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                if (error == ErrorCode.ERR_Unknown_Account) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                } else if (error == ErrorCode.ERR_Unknown_Token) {
                    throw new BrokerException(BrokerErrorCode.TOKEN_NOT_FOUND);
                } else if (error == ErrorCode.ERR_Insufficient_Balance) {
                    throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
                } else if (error == ErrorCode.ERR_Duplicated_Order) {
                    throw new BrokerException(BrokerErrorCode.DUPLICATED_ORDER);
                } else if (error == ErrorCode.ERR_Invalid_Argument) {
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_POSITION_LIMIT) {
                    throw new BrokerException(BrokerErrorCode.ERR_CANCEL_ORDER_POSITION_LIMIT);
                } else {
                    log.error("newOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                    throw new BrokerException(BrokerErrorCode.ORDER_FAILED, e);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new BrokerException(BrokerErrorCode.CREATE_ORDER_TIMEOUT);
            }
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED, e);
        }
    }

    public CancelOrderReply cancelOptionOrder(CancelOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return mock ? MockUtil.cancelOrder() : stub.cancelOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.warn("cancelOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                if (error == ErrorCode.ERR_Unknown_Account) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                } else if (error == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                } else if (error == ErrorCode.ERR_Order_Filled) {
                    throw new BrokerException(BrokerErrorCode.ORDER_HAS_BEEN_FILLED);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_CANCELLED) {
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_CANCELLED);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_FINISHED) {
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FINISHED);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_REJECTED) {
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_REJECTED);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_NOT_FOUND) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_ORDER_BE_LOCKED) {
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_LOCKED);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_UNSUPPORTED_ORDERTYPE) {
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_UNSUPPORTED_ORDER_TYPE);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_MAYBE_ARCHIVED) {
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_ARCHIVED);
                } else if (error == ErrorCode.ORD_4015) {
                    log.warn("cancel order(request: {}) return ORD_4015, return CANCEL_ORDER_ARCHIVED", TextFormat.shortDebugString(request));
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_ARCHIVED);
                } else {
                    log.error("cancelOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                    throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_TIMEOUT);
            }
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
        }
    }

    public BatchCancelOrderReply batchCancelOptionOrder(BatchCancelOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return mock ? MockUtil.batchCancelOrder() : stub.batchCancelOrder(request);
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

    @GrpcLog(printNoResponse = true)
    public GetOrdersReply queryOptionOrders(GetOrdersRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getOrders(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOrderReply getOptionOrderInfo(GetOrderRequest request) {
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
    public GetTradesReply queryOptionMatchInfo(GetTradesRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getTrades(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOrderTradeDetailReply getOptionMatchInfo(GetOrderTradeDetailRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getOrderTradeDetail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public OptionPositionsReply getOptionPositions(OptionPositionsReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getOptionPositions(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public OptionSettlementReply getOptionSettlement(OptionSettlementReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getOptionSettlement(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOptionSettlementListReply getOptionSettlementList(GetOptionSettlementListReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getOptionSettlementList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CreateNewOptionReply createNewOption(CreateNewOptionReq request) {
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(request.getBrokerId());
        try {
            return stub.createNewOption(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

//    public GetOrderFeeRateReply getOrderFeeRate(GetOrderFeeRateReq request) {
//        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub();
//        try {
//            return stub.getOrderFeeRate(request);
//        } catch (StatusRuntimeException e) {
//            log.error("{}", printStatusRuntimeException(e));
//            throw commonStatusRuntimeException(e);
//        }
//    }

    public GetSettlementStatusReply getSettlementStatus(GetSettlementStatusReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getSettlementStatus(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
