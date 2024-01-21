/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/24
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/

package io.bhex.broker.server.grpc.server;

import com.google.protobuf.TextFormat;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.order.*;
import io.bhex.broker.server.grpc.server.service.FuturesOrderService;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class FuturesOrderGrpcService extends FuturesOrderServiceGrpc.FuturesOrderServiceImplBase {

    @Resource
    FuturesOrderService futuresOrderService;

    @Resource(name = "orgRequestHandleTaskExecutor")
    private TaskExecutor orgRequestHandleTaskExecutor;

    @Resource(name = "userRequestHandleTaskExecutor")
    private TaskExecutor userRequestHandleTaskExecutor;

//    @Resource(name = "createOrderV20TaskExecutor")
//    private TaskExecutor createOrderV20TaskExecutor;
//
//    @Resource(name = "cancelOrderV20TaskExecutor")
//    private TaskExecutor cancelOrderV20TaskExecutor;

    @Override
    public void createFuturesOrder(CreateFuturesOrderRequest request, StreamObserver<CreateFuturesOrderResponse> observer) {
        CreateFuturesOrderResponse response;
        try {
            response = futuresOrderService.newFuturesOrder(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).addAllArgs(e.getMessageArgs()).build();
            response = CreateFuturesOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(basicRet)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createFuturesOrder error, request: {}", TextFormat.shortDebugString(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void createFuturesOrderV20(CreateFuturesOrderRequest request, StreamObserver<CreateFuturesOrderResponse> observer) {
//        CompletableFuture.runAsync(() -> {
//            CreateFuturesOrderResponse response;
//            try {
//                response = futuresOrderService.newFuturesOrder(request, false, observer);
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (BrokerException e) {
//                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).addAllArgs(e.getMessageArgs()).build();
//                response = CreateFuturesOrderResponse.newBuilder()
//                        .setRet(e.getCode())
//                        .setBasicRet(basicRet)
//                        .build();
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (Exception e) {
//                log.error("createFuturesOrderV21 error, request: {}", TextFormat.shortDebugString(request), e);
//                observer.onError(e);
//            }
//        }, createOrderV20TaskExecutor);
    }

    @Override
    public void createFuturesOrderV21(CreateFuturesOrderRequest request, StreamObserver<CreateFuturesOrderResponse> observer) {
//        CreateFuturesOrderResponse response;
//        try {
//            response = futuresOrderService.newFuturesOrder(request, true, observer);
//            if (response != null) {
//                observer.onNext(response);
//                observer.onCompleted();
//            }
//        } catch (BrokerException e) {
//            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).addAllArgs(e.getMessageArgs()).build();
//            response = CreateFuturesOrderResponse.newBuilder()
//                    .setRet(e.getCode())
//                    .setBasicRet(basicRet)
//                    .build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (Exception e) {
//            log.error("createFuturesOrderV21 error, request: {}", TextFormat.shortDebugString(request), e);
//            observer.onError(e);
//        }
    }

    @Override
    public void cancelFuturesOrder(CancelFuturesOrderRequest request, StreamObserver<CancelFuturesOrderResponse> observer) {
        CancelFuturesOrderResponse response;
        try {
            response = futuresOrderService.cancelFuturesOrder(request.getHeader(), request.getAccountIndex(),
                    request.getOrderId(), request.getClientOrderId(), request.getFuturesOrderType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CancelFuturesOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancelFuturesOrder error, request: {}", TextFormat.shortDebugString(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelFuturesOrderV20(CancelFuturesOrderRequest request, StreamObserver<CancelFuturesOrderResponse> observer) {
//        CompletableFuture.runAsync(() -> {
//            CancelFuturesOrderResponse response;
//            try {
//                response = futuresOrderService.cancelFuturesOrder(request.getHeader(), request.getAccountIndex(),
//                        request.getOrderId(), request.getClientOrderId(), request.getFuturesOrderType(), false, observer);
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (BrokerException e) {
//                response = CancelFuturesOrderResponse.newBuilder().setRet(e.getCode()).build();
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (Exception e) {
//                log.error("cancelFuturesOrderV20 error, request: {}", TextFormat.shortDebugString(request), e);
//                observer.onError(e);
//            }
//        }, cancelOrderV20TaskExecutor);
    }

    @Override
    public void cancelFuturesOrderV21(CancelFuturesOrderRequest request, StreamObserver<CancelFuturesOrderResponse> observer) {
//        CancelFuturesOrderResponse response;
//        try {
//            response = futuresOrderService.cancelFuturesOrder(request.getHeader(), request.getAccountIndex(),
//                    request.getOrderId(), request.getClientOrderId(), request.getFuturesOrderType(), true, observer);
//            if (response != null) {
//                observer.onNext(response);
//                observer.onCompleted();
//            }
//        } catch (BrokerException e) {
//            response = CancelFuturesOrderResponse.newBuilder().setRet(e.getCode()).build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (Exception e) {
//            log.error("cancelFuturesOrderV21 error, request: {}", TextFormat.shortDebugString(request), e);
//            observer.onError(e);
//        }
    }

    @Override
    public void batchCancelFuturesOrder(BatchCancelOrderRequest request, StreamObserver<BatchCancelOrderResponse> observer) {
        BatchCancelOrderResponse response;
        try {
            Header header = request.getHeader();
            List<String> symbolIds = request.getSymbolIdsList();
            OrderSide orderSide = request.getOrderSide();
            response = futuresOrderService.batchCancelFuturesOrder(header, request.getAccountType(), request.getAccountIndex(), symbolIds, orderSide);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BatchCancelOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("batchCancelFuturesOrder error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getFuturesOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
        GetOrderResponse response;
        try {
            response = futuresOrderService.getFuturesOrderInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                    request.getOrderId(), request.getClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getFuturesOrder error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryFuturesOrders(QueryFuturesOrdersRequest request, StreamObserver<QueryFuturesOrdersResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryFuturesOrdersResponse> observer = (ServerCallStreamObserver<QueryFuturesOrdersResponse>) responseObserver;
            observer.setCompression("gzip");
            QueryFuturesOrdersResponse response;
            try {
                response = futuresOrderService.queryFuturesOrders(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryFuturesOrdersResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryFuturesOrders error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void queryFuturesMatch(QueryMatchRequest request, StreamObserver<QueryMatchResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryMatchResponse> observer = (ServerCallStreamObserver<QueryMatchResponse>) responseObserver;
            observer.setCompression("gzip");
            QueryMatchResponse response;
            try {
                response = futuresOrderService.queryFuturesMatchInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getSymbolId(), request.getFromId(), request.getEndId(),
                        request.getStartTime(), request.getEndTime(), request.getLimit(), request.getOrderSide(), request.getQueryFromEs());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryMatchResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryFuturesMatch error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getFuturesOrderMatch(GetOrderMatchRequest request, StreamObserver<GetOrderMatchResponse> observer) {
        CompletableFuture.runAsync(() -> {
            GetOrderMatchResponse response;
            try {
                response = futuresOrderService.getFuturesOrderMatchInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getOrderId(), request.getFromId(), request.getLimit());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = GetOrderMatchResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("getFuturesOrderMatch error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getFuturesSettlement(FuturesSettlementRequest request, StreamObserver<FuturesSettlementResponse> observer) {
        FuturesSettlementResponse response;
        try {
            response = futuresOrderService.getFuturesSettlement(request.getHeader(),
                    request.getSide(), request.getFromSettlementId(), request.getEndSettlementId(),
                    request.getStartTime(), request.getEndTime(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = FuturesSettlementResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getFuturesSettlement error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setRiskLimit(SetRiskLimitRequest request, StreamObserver<SetRiskLimitResponse> observer) {
        SetRiskLimitResponse response;
        try {
            response = futuresOrderService.setRiskLimit(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SetRiskLimitResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setRiskLimit error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setOrderSetting(SetOrderSettingRequest request, StreamObserver<SetOrderSettingResponse> observer) {
        SetOrderSettingResponse response;
        try {
            response = futuresOrderService.setOrderSetting(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SetOrderSettingResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setOrderSetting error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getRiskLimit(GetRiskLimitRequest request, StreamObserver<GetRiskLimitResponse> observer) {
        GetRiskLimitResponse response;
        try {
            response = futuresOrderService.getRiskLimit(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetRiskLimitResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getRiskLimit error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getOrderSetting(GetOrderSettingRequest request, StreamObserver<GetOrderSettingResponse> observer) {
        GetOrderSettingResponse response;
        try {
            response = futuresOrderService.getOrderSetting(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetOrderSettingResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getOrderSetting error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getFuturesPositions(FuturesPositionsRequest request, StreamObserver<FuturesPositionsResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<FuturesPositionsResponse> observer = (ServerCallStreamObserver<FuturesPositionsResponse>) responseObserver;
            observer.setCompression("gzip");
            FuturesPositionsResponse response;
            try {
                response = futuresOrderService.getFuturesPositions(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = FuturesPositionsResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("getFuturesPositions error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getFuturesTradeAble(GetFuturesTradeAbleRequest request, StreamObserver<GetFuturesTradeAbleResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<GetFuturesTradeAbleResponse> observer = (ServerCallStreamObserver<GetFuturesTradeAbleResponse>) responseObserver;
            observer.setCompression("gzip");
            GetFuturesTradeAbleResponse response;
            try {
                response = futuresOrderService.getFuturesTradeAble(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = GetFuturesTradeAbleResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("getFuturesTradeAble error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getFuturesCoinAsset(GetFuturesCoinAssetRequest request, StreamObserver<GetFuturesCoinAssetResponse> observer) {
        GetFuturesCoinAssetResponse response;
        try {
            response = futuresOrderService.getFuturesCoinAsset(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetFuturesCoinAssetResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getFuturesCoinAsset error", e);
            observer.onError(e);
        }
    }

    @Override
    public void addMargin(AddMarginRequest request, StreamObserver<AddMarginResponse> observer) {
        AddMarginResponse response;
        try {
            response = futuresOrderService.addMargin(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AddMarginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("addMargin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void reduceMargin(ReduceMarginRequest request, StreamObserver<ReduceMarginResponse> observer) {
        ReduceMarginResponse response;
        try {
            response = futuresOrderService.reduceMargin(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ReduceMarginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("reduceMargin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getPlanOrder(GetPlanOrderRequest request, StreamObserver<GetPlanOrderResponse> observer) {
        GetPlanOrderResponse response;
        try {
            response = futuresOrderService.getPlanOrder(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetPlanOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getPlanOrder error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getInsuranceFunds(GetInsuranceFundsRequest request, StreamObserver<GetInsuranceFundsResponse> observer) {
        GetInsuranceFundsResponse response;
        try {
            response = futuresOrderService.getInsuranceFunds(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetInsuranceFundsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getInsuranceFunds error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getFundingRates(GetFundingRatesRequest request, StreamObserver<GetFundingRatesResponse> observer) {
        GetFundingRatesResponse response;
        try {
            response = futuresOrderService.getFundingRates(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetFundingRatesResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getFundingRates error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getHistoryFundingRates(GetHistoryFundingRatesRequest request, StreamObserver<GetHistoryFundingRatesResponse> observer) {
        GetHistoryFundingRatesResponse response;
        try {
            response = futuresOrderService.getHistoryFundingRates(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetHistoryFundingRatesResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getHistoryFundingRates error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getHistoryInsuranceFundBalances(GetHistoryInsuranceFundBalancesRequest request, StreamObserver<GetHistoryInsuranceFundBalancesResponse> observer) {
        GetHistoryInsuranceFundBalancesResponse response;
        try {
            response = futuresOrderService.getHistoryInsuranceFundBalances(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetHistoryInsuranceFundBalancesResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getHistoryInsuranceFundBalances error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getFuturesI18nName(GetFuturesI18nNameRequest request, StreamObserver<GetFuturesI18nNameResponse> observer) {
        GetFuturesI18nNameResponse response;
        try {
            response = futuresOrderService.getFuturesI18nName(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetFuturesI18nNameResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getFuturesI18nName error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getTradeableAndPositions(GetTradeableAndPositionsRequest request, StreamObserver<GetTradeableAndPositionsResponse> responseObserver) {
        ServerCallStreamObserver<GetTradeableAndPositionsResponse> observer = (ServerCallStreamObserver<GetTradeableAndPositionsResponse>) responseObserver;
        observer.setCompression("gzip");
        GetTradeableAndPositionsResponse response;
        try {
            response = futuresOrderService.getTradeableAndPositions(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetTradeableAndPositionsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getTradeableAndPositions error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getFuturesBestOrder(GetFuturesBestOrderRequest request, StreamObserver<GetFuturesBestOrderResponse> observer) {
        GetFuturesBestOrderResponse response;
        try {
            response = futuresOrderService.getFuturesBestOrder(request.getHeader(), request.getExchangeId(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetFuturesBestOrderResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getFuturesBestOrder error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getOrgFuturesPositions(OrgFuturesPositionsRequest request, StreamObserver<OrgFuturesPositionsResponse> observer) {
        CompletableFuture.runAsync(() -> {
            OrgFuturesPositionsResponse response;
            try {
                response = futuresOrderService.getOrgFuturesPositions(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = OrgFuturesPositionsResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("getOrgFuturesPositions error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void calculateProfitInfo(CalculateProfitInfoRequest request, StreamObserver<CalculateProfitInfoResponse> observer) {
        CalculateProfitInfoResponse response;
        try {
            response = futuresOrderService.calculateProfitInfo(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CalculateProfitInfoResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("calculateProfitInfo error", e);
            observer.onError(e);
        }
    }

    @Override
    public void calculateLiquidationPrice(CalculateLiquidationPriceRequest request, StreamObserver<CalculateLiquidationPriceResponse> observer) {
        CalculateLiquidationPriceResponse response;
        try {
            response = futuresOrderService.calculateLiquidationPrice(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CalculateLiquidationPriceResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("calculateLiquidationPrice error", e);
            observer.onError(e);
        }
    }

    public void queryLiquidationPosition(QueryLiquidationPositionRequest request, StreamObserver<QueryLiquidationPositionResponse> observer) {
        CompletableFuture.runAsync(() -> {
            QueryLiquidationPositionResponse response;
            try {
                response = futuresOrderService.getLiquidationPosition(request.getHeader(), request.getAccountId(), request.getOrderId());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryLiquidationPositionResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryLiquidationPosition error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void marketPullFuturesPositions(MarketPullFuturesPositionsRequest request, StreamObserver<MarketPullFuturesPositionsResponse> observer) {
        CompletableFuture.runAsync(() -> {
            MarketPullFuturesPositionsResponse response;
            try {
                response = futuresOrderService.marketPullFuturesPositions(request.getHeader(),
                        request.getSymbolIdsList(), request.getFromPositionId(), request.getLimit());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = MarketPullFuturesPositionsResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("marketPullFuturesPositions error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void setStopProfitLoss(SetStopProfitLossRequest request, StreamObserver<SetStopProfitLossResponse> observer) {
        SetStopProfitLossResponse response;
        try {
            response = futuresOrderService.setStopProfitLoss(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SetStopProfitLossResponse.newBuilder().setRet(e.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setStopProfitLoss error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getStopProfitLoss(GetStopProfitLossRequest request, StreamObserver<GetStopProfitLossResponse> observer) {
        GetStopProfitLossResponse response;
        try {
            StopProfitLossSetting setting = futuresOrderService.getStopProfitLossSetting(
                    request.getHeader(), request.getSymbolId(), request.getIsLong());
            response = GetStopProfitLossResponse.newBuilder().setSetting(setting).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetStopProfitLossResponse.newBuilder().setRet(e.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getStopProfitLoss error", e);
            observer.onError(e);
        }
    }

    @Override
    public void closePromptlyFutureOrder(ClosePromptlyFutureOrderRequest request, StreamObserver<ClosePromptlyFutureOrderResponse> observer) {
        ClosePromptlyFutureOrderResponse response;
        try {
            response = futuresOrderService.closePromptlyFutureOrder(
                    request.getHeader(), request.getSymbolId(), request.getAccountId(), request.getAccountIndex(), request.getClientOrderId(), request.getIsLong(),
                    request.getOrderSource());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            log.warn("close promptly exception " + e.getMessage(), e);
            response = ClosePromptlyFutureOrderResponse.newBuilder()
                    .setRet(e.code())
                    .setBasicRet(BasicRet.newBuilder()
                            .setCode(e.getCode())
                            .setMsg(e.getErrorMessage() == null ? "" : e.getErrorMessage())
                            .addAllArgs(e.getMessageArgs() == null ? new LinkedList<>() : e.getMessageArgs())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getStopProfitLoss error", e);
            observer.onError(e);
        }
    }

    @Override
    public void clearCancelByUser(ClearCancelByUserRequest request, StreamObserver<ClearCancelByUserResponse> responseObserver) {
        try {
            futuresOrderService.clearCancelByUser(request.getHeader());
            responseObserver.onNext(ClearCancelByUserResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            ClearCancelByUserResponse response = ClearCancelByUserResponse.newBuilder().setRet(e.code()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("clearCancelByUser error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void clearCloseByUser(ClearCloseByUserRequest request, StreamObserver<ClearCloseByUserResponse> responseObserver) {
        try {

            futuresOrderService.clearCloseByUser(request.getHeader());
            responseObserver.onNext(ClearCloseByUserResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            ClearCloseByUserResponse response = ClearCloseByUserResponse.newBuilder().setRet(e.code()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("clearCancleByUser error", e);
            responseObserver.onError(e);
        }
    }
}