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

import io.bhex.broker.grpc.app_config.AppDownloadInfos;
import io.grpc.stub.ServerCallStreamObserver;
import org.springframework.core.task.TaskExecutor;

import java.util.concurrent.CompletableFuture;

import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.order.BatchCancelOrderRequest;
import io.bhex.broker.grpc.order.BatchCancelOrderResponse;
import io.bhex.broker.grpc.order.BatchCancelPlanSpotOrderRequest;
import io.bhex.broker.grpc.order.BatchCancelPlanSpotOrderResponse;
import io.bhex.broker.grpc.order.CancelOrderRequest;
import io.bhex.broker.grpc.order.CancelOrderResponse;
import io.bhex.broker.grpc.order.CancelPlanSpotOrderRequest;
import io.bhex.broker.grpc.order.CancelPlanSpotOrderResponse;
import io.bhex.broker.grpc.order.CreateOrderRequest;
import io.bhex.broker.grpc.order.CreateOrderResponse;
import io.bhex.broker.grpc.order.CreatePlanSpotOrderRequest;
import io.bhex.broker.grpc.order.CreatePlanSpotOrderResponse;
import io.bhex.broker.grpc.order.FastCancelOrderRequest;
import io.bhex.broker.grpc.order.FastCancelOrderResponse;
import io.bhex.broker.grpc.order.GetBanSellConfigRequest;
import io.bhex.broker.grpc.order.GetBanSellConfigResponse;
import io.bhex.broker.grpc.order.GetBestOrderRequest;
import io.bhex.broker.grpc.order.GetBestOrderResponse;
import io.bhex.broker.grpc.order.GetDepthInfoRequest;
import io.bhex.broker.grpc.order.GetDepthInfoResponse;
import io.bhex.broker.grpc.order.GetOptionNameRequest;
import io.bhex.broker.grpc.order.GetOptionNameResponse;
import io.bhex.broker.grpc.order.GetOptionSettleStatusRequest;
import io.bhex.broker.grpc.order.GetOptionSettleStatusResponse;
import io.bhex.broker.grpc.order.GetOrderBanConfigRequest;
import io.bhex.broker.grpc.order.GetOrderBanConfigResponse;
import io.bhex.broker.grpc.order.GetOrderFeeRateRequest;
import io.bhex.broker.grpc.order.GetOrderFeeRateResponse;
import io.bhex.broker.grpc.order.GetOrderMatchRequest;
import io.bhex.broker.grpc.order.GetOrderMatchResponse;
import io.bhex.broker.grpc.order.GetOrderRequest;
import io.bhex.broker.grpc.order.GetOrderResponse;
import io.bhex.broker.grpc.order.GetPlanSpotOrderRequest;
import io.bhex.broker.grpc.order.GetPlanSpotOrderResponse;
import io.bhex.broker.grpc.order.HistoryOptionRequest;
import io.bhex.broker.grpc.order.HistoryOptionsResponse;
import io.bhex.broker.grpc.order.OptionPositionsRequest;
import io.bhex.broker.grpc.order.OptionPositionsResponse;
import io.bhex.broker.grpc.order.OptionSettlementRequest;
import io.bhex.broker.grpc.order.OptionSettlementResponse;
import io.bhex.broker.grpc.order.OrderServiceGrpc;
import io.bhex.broker.grpc.order.OrgBatchCancelOrderRequest;
import io.bhex.broker.grpc.order.OrgBatchCancelOrderResponse;
import io.bhex.broker.grpc.order.QueryAnyOrdersRequest;
import io.bhex.broker.grpc.order.QueryMatchRequest;
import io.bhex.broker.grpc.order.QueryMatchResponse;
import io.bhex.broker.grpc.order.QueryOrdersRequest;
import io.bhex.broker.grpc.order.QueryOrdersResponse;
import io.bhex.broker.grpc.order.QueryPlanSpotOrdersRequest;
import io.bhex.broker.grpc.order.QueryPlanSpotOrdersResponse;
import io.bhex.broker.server.grpc.server.service.OptionOrderService;
import io.bhex.broker.server.grpc.server.service.OptionService;
import io.bhex.broker.server.grpc.server.service.OrderService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    @Resource
    private OrderService orderService;

    @Resource
    private OptionOrderService optionOrderService;

    @Resource
    private OptionService optionService;

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
    public void createOrder(CreateOrderRequest request, StreamObserver<CreateOrderResponse> observer) {
        CreateOrderResponse response;
        try {
            BaseResult<CreateOrderResponse> baseResult = orderService.newOrder(request.getHeader(), request.getAccountIndex(), request.getSymbolId().toUpperCase(),
                    request.getClientOrderId(), request.getOrderType(), request.getOrderSide(), request.getPrice(), request.getQuantity(), request.getTimeInForce(), request.getOrderSource(),
                    request.getAccountType());
            if (baseResult.isSuccess()) {
                response = baseResult.getData();
            } else {
                response = CreateOrderResponse.newBuilder()
                        .setRet(baseResult.getCode())
                        .setBasicRet(BasicRet.newBuilder().setCode(baseResult.getCode())
                                .setMsg(baseResult.getMsg())
                                .addArgs(baseResult.getExtendInfo() == null ? "" : baseResult.getExtendInfo().toString())
                                .build())
                        .build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder()
                            .setCode(e.getCode())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("create order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createOrderV20(CreateOrderRequest request, StreamObserver<CreateOrderResponse> observer) {
//        CompletableFuture.runAsync(() -> {
//            CreateOrderResponse response;
//            try {
//                BaseResult<CreateOrderResponse> baseResult = orderService.newOrder(request.getHeader(), request.getAccountIndex(), request.getSymbolId().toUpperCase(),
//                        request.getClientOrderId(), request.getOrderType(), request.getOrderSide(), request.getPrice(), request.getQuantity(), request.getTimeInForce(), request.getOrderSource(),
//                        false, observer, request.getAccountType());
//                if (baseResult.isSuccess()) {
//                    response = baseResult.getData();
//                } else {
//                    response = CreateOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
//                }
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (BrokerException e) {
//                response = CreateOrderResponse.newBuilder().setRet(e.getCode()).build();
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (Exception e) {
//                log.error("create order error", e);
//                observer.onError(e);
//            }
//        }, createOrderV20TaskExecutor);
    }

    @Override
    public void createOrderV21(CreateOrderRequest request, StreamObserver<CreateOrderResponse> observer) {
//        CreateOrderResponse response;
//        try {
//            BaseResult<CreateOrderResponse> baseResult = orderService.newOrder(request.getHeader(), request.getAccountIndex(), request.getSymbolId().toUpperCase(),
//                    request.getClientOrderId(), request.getOrderType(), request.getOrderSide(), request.getPrice(), request.getQuantity(), request.getTimeInForce(), request.getOrderSource(),
//                    true, observer, request.getAccountType());
//            if (baseResult != null) {
//                if (baseResult.isSuccess()) {
//                    response = baseResult.getData();
//                } else {
//                    response = CreateOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
//                }
//                observer.onNext(response);
//                observer.onCompleted();
//            }
//        } catch (BrokerException e) {
//            response = CreateOrderResponse.newBuilder().setRet(e.getCode()).build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (Exception e) {
//            log.error("create order error", e);
//            observer.onError(e);
//        }
    }

    @Override
    public void getOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
        GetOrderResponse response;
        try {
            response = orderService.getOrderInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(), request.getOrderId(), request.getClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("get order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> observer) {
        CancelOrderResponse response;
        try {
            BaseResult<CancelOrderResponse> baseResult = orderService.cancelOrder(request.getHeader(), request.getAccountIndex(),
                    request.getOrderId(), request.getClientOrderId(), request.getAccountType());
            if (baseResult.isSuccess()) {
                response = baseResult.getData();
            } else {
                response = CancelOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CancelOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancel order error", e);
            observer.onError(e);
        }
    }


    @Override
    public void fastCancelOrder(FastCancelOrderRequest request, StreamObserver<FastCancelOrderResponse> observer) {
        FastCancelOrderResponse response;
        try {
            BaseResult<FastCancelOrderResponse> baseResult = orderService.fastCancelOrder(request.getHeader(),
                    request.getOrderId(), request.getClientOrderId(), request.getSymbolId(), request.getSecurityType(), request.getAccountIndex(), request.getAccountType());
            if (baseResult.isSuccess()) {
                response = baseResult.getData();
            } else {
                response = FastCancelOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = FastCancelOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("fast cancel order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelOrderV20(CancelOrderRequest request, StreamObserver<CancelOrderResponse> observer) {
//        CompletableFuture.runAsync(() -> {
//            CancelOrderResponse response;
//            try {
//                BaseResult<CancelOrderResponse> baseResult = orderService.cancelOrder(request.getHeader(), request.getAccountIndex(),
//                        request.getOrderId(), request.getClientOrderId(), false, observer, request.getAccountType());
//                if (baseResult.isSuccess()) {
//                    response = baseResult.getData();
//                } else {
//                    response = CancelOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
//                }
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (BrokerException e) {
//                response = CancelOrderResponse.newBuilder().setRet(e.getCode()).build();
//                observer.onNext(response);
//                observer.onCompleted();
//            } catch (Exception e) {
//                log.error("cancel order error", e);
//                observer.onError(e);
//            }
//        }, cancelOrderV20TaskExecutor);
    }

    @Override
    public void cancelOrderV21(CancelOrderRequest request, StreamObserver<CancelOrderResponse> observer) {
//        CancelOrderResponse response;
//        try {
//            BaseResult<CancelOrderResponse> baseResult = orderService.cancelOrder(request.getHeader(), request.getAccountIndex(),
//                    request.getOrderId(), request.getClientOrderId(), false, observer, request.getAccountType());
//            if (baseResult != null) {
//                if (baseResult.isSuccess()) {
//                    response = baseResult.getData();
//                } else {
//                    response = CancelOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
//                }
//                observer.onNext(response);
//                observer.onCompleted();
//            }
//        } catch (BrokerException e) {
//            response = CancelOrderResponse.newBuilder().setRet(e.getCode()).build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (Exception e) {
//            log.error("cancel order error", e);
//            observer.onError(e);
//        }
    }

    @Override
    public void batchCancelOrder(BatchCancelOrderRequest
                                         request, StreamObserver<BatchCancelOrderResponse> observer) {
        BatchCancelOrderResponse response;
        try {
            response = orderService.batchCancelOrder(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                    request.getSymbolIdsList(), request.getOrderSide());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BatchCancelOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder()
                            .setCode(e.getCode())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("batch cancel order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void orgBatchCancelOrder(OrgBatchCancelOrderRequest
                                            request, StreamObserver<OrgBatchCancelOrderResponse> observer) {
        OrgBatchCancelOrderResponse response;
        try {
            response = orderService.orgBatchCancelOrder(request.getHeader(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = OrgBatchCancelOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder()
                            .setCode(e.getCode())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("batch cancel order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryOrders(QueryOrdersRequest request, StreamObserver<QueryOrdersResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryOrdersResponse> observer = (ServerCallStreamObserver<QueryOrdersResponse>) responseObserver;
            observer.setCompression("gzip");
            QueryOrdersResponse response;
            try {
                response = orderService.queryOrders(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getSymbolId(), request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(),
                        request.getBaseTokenId(), request.getQuoteTokenId(), request.getOrderType(), request.getOrderSide(),
                        request.getLimit(), request.getQueryType());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryOrdersResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("query orders error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void queryAnyOrders(QueryAnyOrdersRequest request, StreamObserver<QueryOrdersResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryOrdersResponse> observer = (ServerCallStreamObserver<QueryOrdersResponse>) responseObserver;
            observer.setCompression("gzip");
            QueryOrdersResponse response;
            try {
                response = orderService.queryAnyOrders(request.getHeader(), request.getAnyAccountId(), request.getAccountType(), request.getAccountIndex(),
                        request.getSymbolId(), request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(),
                        request.getOrderType(), request.getOrderSide(), request.getLimit(), request.getQueryType());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryOrdersResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("query AnyOrders error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void queryMatch(QueryMatchRequest request, StreamObserver<QueryMatchResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryMatchResponse> observer = (ServerCallStreamObserver<QueryMatchResponse>) responseObserver;
            observer.setCompression("gzip");
            QueryMatchResponse response;
            try {
                response = orderService.queryMatchInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getSymbolId(), request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(),
                        request.getLimit(), request.getOrderSide(), request.getQueryFromEs());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryMatchResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("query match error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getOrderMatch(GetOrderMatchRequest request, StreamObserver<GetOrderMatchResponse> observer) {
        CompletableFuture.runAsync(() -> {
            GetOrderMatchResponse response;
            try {
                response = orderService.getOrderMatchInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getOrderId(), request.getFromId(), request.getLimit());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = GetOrderMatchResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("get order match error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getBestOrder(GetBestOrderRequest request, StreamObserver<GetBestOrderResponse> responseObserver) {
        ServerCallStreamObserver<GetBestOrderResponse> observer = (ServerCallStreamObserver<GetBestOrderResponse>) responseObserver;
        observer.setCompression("gzip");
        GetBestOrderResponse response;
        try {
            response = orderService.getBestOrder(request.getHeader(), request.getExchangeId(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("get best order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getBestDepth(GetDepthInfoRequest request, StreamObserver<GetDepthInfoResponse> responseObserver) {
        ServerCallStreamObserver<GetDepthInfoResponse> observer = (ServerCallStreamObserver<GetDepthInfoResponse>) responseObserver;
        observer.setCompression("gzip");
        GetDepthInfoResponse response;
        try {
            response = orderService.getDepthInfo(request.getHeader(), request.getExchangeId(), request.getSymbolIdList());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("get depth info error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createOptionOrder(CreateOrderRequest request, StreamObserver<CreateOrderResponse> observer) {
        CreateOrderResponse response;
        try {
            response = optionOrderService.newOptionOrder(
                    request.getHeader(),
                    request.getAccountType(),
                    request.getAccountIndex(),
                    request.getExchangeId(),
                    request.getSymbolId(),
                    request.getClientOrderId(),
                    request.getOrderType(),
                    request.getOrderSide(),
                    request.getPrice(),
                    request.getQuantity(),
                    request.getTimeInForce(),
                    request.getOrderSource()
            );
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).addAllArgs(e.getMessageArgs()).build();
            response = CreateOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(basicRet)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createOptionOrder error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getOptionOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
        GetOrderResponse response;
        try {
            response = optionOrderService.getOptionOrderInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                    request.getOrderId(), request.getClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("get order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelOptionOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> observer) {
        CancelOrderResponse response;
        try {
            response = optionOrderService.cancelOptionOrder(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                    request.getOrderId(), request.getClientOrderId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CancelOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder()
                            .setCode(e.getCode())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancel order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void batchCancelOptionOrder(BatchCancelOrderRequest
                                               request, StreamObserver<BatchCancelOrderResponse> observer) {
        BatchCancelOrderResponse response;
        try {
            response = optionOrderService.batchCancelOptionOrder(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                    request.getSymbolIdsList(), request.getOrderSide());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BatchCancelOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder()
                            .setCode(e.getCode())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("batch cancel order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getHistoryOptions(HistoryOptionRequest request, StreamObserver<HistoryOptionsResponse> observer) {
        CompletableFuture.runAsync(() -> {
            HistoryOptionsResponse response;
            try {
                response = optionOrderService.getHistoryOptions(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = HistoryOptionsResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("batch cancel order error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void queryOptionMatch(QueryMatchRequest request, StreamObserver<QueryMatchResponse> observer) {
        CompletableFuture.runAsync(() -> {
            QueryMatchResponse response;
            try {
                response = optionOrderService.queryOptionMatchInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(), request.getSymbolId(),
                        request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(), request.getLimit(),
                        request.getOrderSide(), request.getQueryFromEs());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryMatchResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("query match error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getOptionOrderMatch(GetOrderMatchRequest request, StreamObserver<GetOrderMatchResponse> observer) {
        CompletableFuture.runAsync(() -> {
            GetOrderMatchResponse response;
            try {
                response = optionOrderService.getOptionOrderMatchInfo(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getOrderId(), request.getFromId(), request.getLimit());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = GetOrderMatchResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("get order match error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void queryOptionOrders(QueryOrdersRequest request, StreamObserver<QueryOrdersResponse> observer) {
        CompletableFuture.runAsync(() -> {
            QueryOrdersResponse response;
            try {
                response = optionOrderService.queryOptionOrders(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getSymbolId(), request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(),
                        request.getBaseTokenId(), request.getQuoteTokenId(), request.getOrderType(), request.getOrderSide(),
                        request.getLimit(), request.getQueryType(), request.getOrderStatusList());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryOrdersResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("query orders error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }


    @Override
    public void getOptionPositions(OptionPositionsRequest
                                           request, StreamObserver<OptionPositionsResponse> observer) {
        CompletableFuture.runAsync(() -> {
            OptionPositionsResponse response;
            try {
                response = optionOrderService.getOptionPositions(request.getHeader(), request.getAccountType(), request.getAccountIndex(), request.getTokenIds(),
                        request.getExchangeId(), request.getFromBalanceId(), request.getEndBalanceId(), request.getLimit());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = OptionPositionsResponse.newBuilder().build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("batch cancel order error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getOptionSettlement(OptionSettlementRequest
                                            request, StreamObserver<OptionSettlementResponse> observer) {
        OptionSettlementResponse response;
        try {
            response = optionOrderService.getOptionSettlement(request.getHeader(), request.getAccountType(), request.getAccountIndex(), request.getSide(),
                    request.getFromSettlementId(), request.getEndSettlementId(), request.getStartTime(), request.getEndTime(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = OptionSettlementResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("batch cancel order error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getOptionName(GetOptionNameRequest
                                      request, StreamObserver<GetOptionNameResponse> responseObserver) {
        GetOptionNameResponse response;
        try {
            responseObserver.onNext(optionService.getOptionName(request.getInKey(), request.getEnv()));
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOptionNameResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOptionName error", e);
            responseObserver.onError(e);
        }
    }


    @Override
    public void getOrderFeeRate(GetOrderFeeRateRequest
                                        request, StreamObserver<GetOrderFeeRateResponse> responseObserver) {
        GetOrderFeeRateResponse response;
        try {
            GetOrderFeeRateResponse reply = optionOrderService.getOrderFeeRate(request.getHeader(), request.getExchangeId(), request.getSymbolIdsList());
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOrderFeeRateResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOrderFeeRate error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOptionSettleStatus(GetOptionSettleStatusRequest request,
                                      StreamObserver<GetOptionSettleStatusResponse> responseObserver) {
        try {

            GetOptionSettleStatusResponse reply = optionOrderService.getOptionSettleStatus(request.getSymbolIdsList());
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOptionSettleStatus error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getBanSellConfig(GetBanSellConfigRequest request,
                                 StreamObserver<GetBanSellConfigResponse> observer) {
        GetBanSellConfigResponse response;
        try {
            response = orderService.getBanSellConfig(request.getHeader().getUserId(), request.getOrgId(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBanSellConfigResponse.newBuilder().setCanSell(false).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void getOrderBanConfig(GetOrderBanConfigRequest
                                          request, StreamObserver<GetOrderBanConfigResponse> observer) {
        GetOrderBanConfigResponse response;
        try {
            response = orderService.getOrderBanConfig(request.getHeader().getUserId(), request.getOrgId(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetOrderBanConfigResponse.newBuilder().setCanBuy(false).setCanSell(false).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void createPlanSpotOrder(CreatePlanSpotOrderRequest request, StreamObserver<CreatePlanSpotOrderResponse> observer) {
        CreatePlanSpotOrderResponse response;
        try {
            BaseResult<CreatePlanSpotOrderResponse> baseResult = orderService.createPlanSpotOrder(request);
            if (baseResult.isSuccess()) {
                response = baseResult.getData();
            } else {
                response = CreatePlanSpotOrderResponse.newBuilder()
                        .setRet(baseResult.getCode())
                        .setBasicRet(BasicRet.newBuilder().setCode(baseResult.getCode())
                                .setMsg(baseResult.getMsg())
                                .addArgs(baseResult.getExtendInfo() == null ? "" : baseResult.getExtendInfo().toString())
                                .build()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).addAllArgs(e.getMessageArgs()).build();
            response = CreatePlanSpotOrderResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(basicRet)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createPlanSpotOrder error, request: {}", TextFormat.shortDebugString(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelPlanSpotOrder(CancelPlanSpotOrderRequest request, StreamObserver<CancelPlanSpotOrderResponse> observer) {
        CancelPlanSpotOrderResponse response;
        try {
            log.info("cancelPlanSpotOrder! orgId:{},accountId:{},orderId:{},clientOrderId:{}", request.getHeader().getOrgId()
                    , request.getAccountId(), request.getOrderId(), request.getClientOrderId());
            BaseResult<CancelPlanSpotOrderResponse> baseResult = orderService.cancelPlanSpotOrder(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                    request.getOrderId(), request.getClientOrderId());
            if (baseResult.isSuccess()) {
                response = baseResult.getData();
            } else {
                response = CancelPlanSpotOrderResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CancelPlanSpotOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancelPlanSpotOrder error, request: {}", TextFormat.shortDebugString(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void batchCancelPlanSpotOrder(BatchCancelPlanSpotOrderRequest request, StreamObserver<BatchCancelPlanSpotOrderResponse> observer) {
        BatchCancelPlanSpotOrderResponse response;
        try {
            log.info("batchCancelPlanSpotOrder! orgId:{},accountId:{},symbolId:{},orderSide:{}", request.getHeader().getOrgId()
                    , request.getAccountId(), request.getSymbolId(), request.getOrderSide().name());
            response = orderService.batchCancelPlanSpotOrder(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                    request.getSymbolId(), request.getOrderSide());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BatchCancelPlanSpotOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("batchCancelPlanSpotOrder error, request: {}", TextFormat.shortDebugString(request), e);
            observer.onError(e);
        }
    }

    @Override
    public void getPlanSpotOrder(GetPlanSpotOrderRequest request, StreamObserver<GetPlanSpotOrderResponse> observer) {
        GetPlanSpotOrderResponse response;
        try {
            log.info("adminGetPlanSpotOrder! orgId:{},accountId:{},orderId:{},clientOrderId:{}", request.getHeader().getOrgId()
                    , request.getAccountId(), request.getOrderId(), request.getClientOrderId());
            response = orderService.getPlanSpotOrder(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetPlanSpotOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getPlanSpotOrder error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryPlanSpotOrders(QueryPlanSpotOrdersRequest request, StreamObserver<QueryPlanSpotOrdersResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            log.info("queryPlanSpotOrders! orgId:{},accountId:{},startOrderId:{},endOrderId:{},startTime:{},endTime:{},side:{},limit:{}", request.getHeader().getOrgId()
                    , request.getAccountId(), request.getFromOrderId(), request.getEndOrderId(), request.getStartTime(), request.getEndTime(), request.getOrderSide(), request.getLimit());
            ServerCallStreamObserver<QueryPlanSpotOrdersResponse> observer = (ServerCallStreamObserver<QueryPlanSpotOrdersResponse>) responseObserver;
            observer.setCompression("gzip");
            QueryPlanSpotOrdersResponse response;
            try {
                response = orderService.queryPlanSpotOrders(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryPlanSpotOrdersResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryPlanSpotOrders error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void adminGetPlanSpotOrder(GetPlanSpotOrderRequest request, StreamObserver<GetPlanSpotOrderResponse> observer) {
        GetPlanSpotOrderResponse response;
        try {
            log.info("adminGetPlanSpotOrder! orgId:{},accountId:{},orderId:{},clientOrderId:{}", request.getHeader().getOrgId()
                    , request.getAccountId(), request.getOrderId(), request.getClientOrderId());
            response = orderService.adminGetPlanSpotOrder(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetPlanSpotOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("adminGetPlanSpotOrder error", e);
            observer.onError(e);
        }
    }

    @Override
    public void adminQueryPlanSpotOrders(QueryPlanSpotOrdersRequest request, StreamObserver<QueryPlanSpotOrdersResponse> observer) {
        CompletableFuture.runAsync(() -> {
            log.info("adminQueryPlanSpotOrders! orgId:{},accountId:{},startOrderId:{},endOrderId:{},limit:{}", request.getHeader().getOrgId()
                    , request.getAccountId(), request.getFromOrderId(), request.getEndOrderId(), request.getLimit());
            QueryPlanSpotOrdersResponse response;
            try {
                response = orderService.adminQueryPlanSpotOrders(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryPlanSpotOrdersResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("adminQueryPlanSpotOrders error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }
}
