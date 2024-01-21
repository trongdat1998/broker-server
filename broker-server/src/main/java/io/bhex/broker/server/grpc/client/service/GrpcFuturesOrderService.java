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
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.token.SymbolDetail;
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
public class GrpcFuturesOrderService extends GrpcBaseService {

    private static final boolean mock = false;

    public NewOrderReply createFuturesOrder(NewOrderRequest request, SymbolDetail symbolDetail) {
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(request.getOrgId());
        try {
            return mock ? MockUtil.createOrder() : stub.newFuturesOrder(request);
        } catch (StatusRuntimeException e) {
            handleCreateOrderStatusRuntimeException(request, symbolDetail, false, e);
            return null;
        }
    }

    public void handleCreateOrderStatusRuntimeException(NewOrderRequest request, SymbolDetail symbolDetail, boolean isAsync, StatusRuntimeException e) {
        ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
        if (errorStatus != null) {
            ErrorCode error = errorStatus.getCode();
            log.warn("{} failed: {}, reason:{}, order -> accountId:{} symbol:{} cid:{} \n (request: {})",
                    isAsync ? "asyncNewFuturesOrder" : "newFuturesOrder", error.name(), errorStatus.getReason(),
                    request.getAccountId(), request.getSymbolId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
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
            } else if (error == ErrorCode.ERR_QUANTITY_INVALID) {
                throw new BrokerException(BrokerErrorCode.ORDER_FUTURES_QUANTITY_INVALID);
            } else if (error == ErrorCode.ERR_CANCEL_ORDER_POSITION_LIMIT) {
                throw new BrokerException(BrokerErrorCode.ERR_CANCEL_ORDER_POSITION_LIMIT);
            } else if (error == ErrorCode.ERR_CANCEL_ORDER_LEVERAGE_INVALID) {
                throw new BrokerException(BrokerErrorCode.ORDER_FUTURES_LEVERAGE_INVALID);
            } else if (error == ErrorCode.ERR_MAYBE_TRIGGER_LIQUI) {
                throw new BrokerException(BrokerErrorCode.ORDER_MAYBE_TRIGGER_LIQ);
            } else if (error == ErrorCode.ERR_PRICE_INVALID) {
                if (!request.getExtraFlag().equals(NewOrderRequest.ExtraFlagEnum.MARKET_PRICE)) {
                    // 反向合约需要反转买卖方向
                    OrderSideEnum side;
                    if (symbolDetail.getIsReverse()) {
                        side = request.getSide().equals(OrderSideEnum.SELL) ? OrderSideEnum.BUY : OrderSideEnum.SELL;
                    } else {
                        side = request.getSide();
                    }
                    if (side.equals(OrderSideEnum.SELL) && request.getIsClose()) {
                        // 非市价平多仓提示价格过低
                        throw new BrokerException(BrokerErrorCode.ORDER_PRICE_UNDER_LIQ);
                    } else if (side.equals(OrderSideEnum.BUY) && request.getIsClose()) {
                        // 非市价平空仓提示价格过高
                        throw new BrokerException(BrokerErrorCode.ORDER_PRICE_OUT_OF_LIQ);
                    }
                }
                throw new BrokerException(BrokerErrorCode.ORDER_PRICE_ILLEGAL);
            } else {
                log.error("{} failed: {} (request: {})", isAsync ? "asyncNewFuturesOrder" : "newFuturesOrder", error.name(), TextFormat.shortDebugString(request));
                throw new BrokerException(BrokerErrorCode.ORDER_FAILED, e);
            }
        }
        log.error("{} failed: {} order -> accountId:{} symbol:{} cid:{} \n (request: {})", isAsync ? "asyncNewFuturesOrder" : "newFuturesOrder", e.getStatus().getCode(),
                request.getAccountId(), request.getSymbolId(), request.getClientOrderId(), TextFormat.shortDebugString(request), e);
        if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
            throw new BrokerException(BrokerErrorCode.CREATE_ORDER_TIMEOUT);
        }
        throw new BrokerException(BrokerErrorCode.ORDER_FAILED, e);
    }

    public CancelOrderReply cancelFuturesOrder(CancelOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            if (mock) {
                return MockUtil.cancelOrder();
            }
            CancelOrderReply reply = stub.cancelOrder(request);
            if (reply.getCode() != ErrorCode.SUCCESS) {
                handleCancelOrderException(request, reply.getCode(), false, null);
                return null;
            }
            return reply;
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                handleCancelOrderException(request, error, false, errorStatus);
                return null;
            }
            log.error("cancelFuturesOrder failed: {}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                    e.getStatus().getCode(), request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_TIMEOUT);
            }
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED, e);
        }
    }

    public void handleCancelOrderException(CancelOrderRequest request, ErrorCode errorCode, boolean isAsync, ErrorStatus errorStatus) {
        log.warn("{} failed: {}, reason:{}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                isAsync ? "asyncCancelFuturesOrder" : "cancelFuturesOrder", errorCode.name(), errorStatus == null ? "" : errorStatus.getReason(),
                request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
        if (errorCode == ErrorCode.ERR_Unknown_Account) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        } else if (errorCode == ErrorCode.ERR_Order_Not_Found) {
            throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
        } else if (errorCode == ErrorCode.ERR_Order_Filled) {
            throw new BrokerException(BrokerErrorCode.ORDER_HAS_BEEN_FILLED);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_CANCELLED) {
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_CANCELLED);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_FINISHED) {
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FINISHED);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_REJECTED) {
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_REJECTED);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_NOT_FOUND) {
            throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_ORDER_BE_LOCKED) {
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_LOCKED);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_UNSUPPORTED_ORDERTYPE) {
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_UNSUPPORTED_ORDER_TYPE);
        } else if (errorCode == ErrorCode.ERR_CANCEL_ORDER_MAYBE_ARCHIVED) {
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_ARCHIVED);
        } else if (errorCode == ErrorCode.ORD_4015) {
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_ARCHIVED);
        } else {
            log.error("{} failed: {}, reason:{}, order -> accountId:{} orderId:{} cid:{} \n (request: {})",
                    isAsync ? "asyncCancelFuturesOrder" : "cancelFuturesOrder", errorCode.name(), errorStatus == null ? "" : errorStatus.getReason(),
                    request.getAccountId(), request.getOrderId(), request.getClientOrderId(), TextFormat.shortDebugString(request));
            throw new BrokerException(BrokerErrorCode.CANCEL_ORDER_FAILED);
        }
    }

    public BatchCancelOrderReply batchCancelFuturesOrder(BatchCancelOrderRequest request) {
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
    public GetOrdersReply queryFuturesOrders(GetOrdersRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.shortDeadlineOrderServiceBlockingStub(orgId);
        try {
            return stub.getOrders(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public FuturesPositionsResponse getFuturesPositions(FuturesPositionsRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        FuturesServerGrpc.FuturesServerBlockingStub stub = grpcClientConfig.shortDeadlineFuturesServerStub(orgId);
        try {
            return stub.getFuturesPositions(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetRiskLimitResponse setRiskLimit(SetRiskLimitRequest request) {
        try {
            return grpcClientConfig.futuresServerStub(request.getBrokerId()).setRiskLimit(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.error("setRiskLimit failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                if (error == ErrorCode.ERR_MAYBE_TRIGGER_LIQUI) {
                    throw new BrokerException(BrokerErrorCode.SET_RISKLIMIT_MAYBE_TRIGGER_LIQUI);
                } else if (error == ErrorCode.ERR_CANCEL_ORDER_POSITION_LIMIT) {
                    throw new BrokerException(BrokerErrorCode.SET_RISKLIMIT_POS_NOT_ENOUGH);
                }
            }
            log.error("setRiskLimit error: {}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetOrderSettingResponse setOrderSetting(SetOrderSettingRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).setOrderSetting(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetRiskLimitResponse getRiskLimit(GetRiskLimitRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).getRiskLimit(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOrderSettingResponse getOrderSetting(GetOrderSettingRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).getOrderSetting(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOrderReply getFuturesOrderInfo(GetOrderRequest request) {
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
    public GetTradesReply queryFuturesMatchInfo(GetTradesRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.shortDeadlineOrderServiceBlockingStub(orgId);
        try {
            return stub.getTrades(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetOrderTradeDetailReply getFuturesMatchInfo(GetOrderTradeDetailRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.getOrderTradeDetail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public OptionSettlementReply getFuturesSettlement(OptionSettlementReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getOptionSettlement(request);//todo: use futures settlement grpc
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public NewPlanOrderReply createFuturesPlanOrder(NewPlanOrderRequest request) {
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(request.getOrgId());
        try {
            return stub.newPlanOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.warn("newOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                if (error == ErrorCode.ERR_Unknown_Account) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                } else if (error == ErrorCode.ERR_Unknown_Token) {
                    throw new BrokerException(BrokerErrorCode.TOKEN_NOT_FOUND);
                } else if (error == ErrorCode.ERR_Duplicated_Order) {
                    throw new BrokerException(BrokerErrorCode.DUPLICATED_ORDER);
                } else if (error == ErrorCode.ERR_Invalid_Argument) {
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                } else {
                    log.error("newPlanOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
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

    public CancelPlanOrderReply cancelFuturesPlanOrder(CancelPlanOrderRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OrderServiceGrpc.OrderServiceBlockingStub stub = grpcClientConfig.orderServiceBlockingStub(orgId);
        try {
            return stub.cancelPlanOrder(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.warn("cancelFuturesPlanOrder failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
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

    public AddMarginResponse addMargin(AddMarginRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).addMargin(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                log.warn("addMargin error. failed:{}, request: {}", errorStatus.getCode().name(), TextFormat.shortDebugString(request));
                if (errorStatus.getCode() == ErrorCode.ERR_Insufficient_Balance) {
                    throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
                } else if (errorStatus.getCode() == ErrorCode.ERR_REDUCE_MARGIN_VALUE_INVALID) {
                    throw new BrokerException(BrokerErrorCode.REDUCE_MARGIN_INVALID);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ReduceMarginResponse reduceMargin(ReduceMarginRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).reduceMargin(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                log.warn("reduceMargin error. failed:{}, request: {}", errorStatus.getCode().name(), TextFormat.shortDebugString(request));
                if (errorStatus.getCode() == ErrorCode.ERR_Insufficient_Balance) {
                    throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @GrpcLog(printNoResponse = true)
    public GetPlanOrdersReply getPlanOrders(GetPlanOrdersRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.orderServiceBlockingStub(orgId).getPlanOrders(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetPlanOrderReply getPlanOrder(GetPlanOrderRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.orderServiceBlockingStub(orgId).getPlanOrder(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetInsuranceFundsResponse getInsuranceFunds(GetInsuranceFundsRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).getInsuranceFunds(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetFundingRatesResponse getFundingRates(GetFundingRatesRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).getFundingRates(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetHistoryFundingRatesResponse getHistoryFundingRates(GetHistoryFundingRatesRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).getHistoryFundingRates(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetInsuranceFundConfigResponse getInsuranceFundConfig(GetInsuranceFundConfigRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).getInsuranceFundConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetLiquidationPositionResponse getLiquidationPosition(GetLiquidationPositionRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).getLiquidationPosition(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                if (errorStatus.getCode() == ErrorCode.ERR_Order_Not_Found) {
                    throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
                } else if (errorStatus.getCode() == ErrorCode.ERR_Invalid_Argument) {
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    /**
     * 拉取用户持仓仓位信息
     * Note: 这个接口仅限于特殊OPENAPI用户调用
     */
    public PullFuturesPositionoReply pullFuturesPosition(PullFuturesPositionRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return grpcClientConfig.futuresServerStub(orgId).pullFuturesPosition(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
