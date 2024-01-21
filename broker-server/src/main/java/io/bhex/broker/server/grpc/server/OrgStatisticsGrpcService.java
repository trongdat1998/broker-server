package io.bhex.broker.server.grpc.server;

import com.google.common.base.Strings;
import io.bhex.base.account.ExtraFlag;
import io.bhex.base.account.NewOrderRequest;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.token.SymbolDetail;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.order.Order;
import io.bhex.broker.grpc.order.OrderType;
import io.bhex.broker.grpc.statistics.*;
import io.bhex.broker.server.elasticsearch.entity.BalanceFlow;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.FuturesOrderService;
import io.bhex.broker.server.grpc.server.service.OrgStatisticsService;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.util.DecimalStringFormatter;
import io.bhex.broker.server.util.FuturesUtil;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OrgStatisticsGrpcService extends StatisticsServiceGrpc.StatisticsServiceImplBase {

    @Resource(name = "statisticsTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private OrgStatisticsService orgStatisticsService;

    @Resource
    private BasicService basicService;

    @Resource
    private AccountService accountService;

    @Resource
    private FuturesOrderService futuresOrderService;

    @Override
    public void queryColdWalletBalance(QueryColdWalletBalanceRequest request, StreamObserver<QueryColdWalletBalanceResponse> observer) {
        CompletableFuture.runAsync(() -> {
            Header header = request.getHeader();
            QueryColdWalletBalanceResponse response;
            try {
                response = orgStatisticsService.queryColdWalletBalanceInfo(header.getOrgId());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryColdWalletBalanceResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryColdWalletBalance error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryOrgBalanceSummaryInfo(QueryOrgBalanceSummaryInfoRequest request, StreamObserver<QueryOrgBalanceSummaryInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            Header header = request.getHeader();
            QueryOrgBalanceSummaryInfoResponse response;
            try {
                List<OrgBalanceSummary> balanceSummaryList = orgStatisticsService.orgBalanceSummary(header.getOrgId());
                List<QueryOrgBalanceSummaryInfoResponse.OrgBalanceSummaryInfo> tokenHoldInfoList = balanceSummaryList.stream()
                        .map(balance -> QueryOrgBalanceSummaryInfoResponse.OrgBalanceSummaryInfo.newBuilder()
                                .setTokenId(balance.getTokenId())
                                .setTotal(balance.getTotalSum().stripTrailingZeros().toPlainString())
                                .setAvailable(balance.getAvailableSum().stripTrailingZeros().toPlainString())
                                .setLock(balance.getLockedTotal().stripTrailingZeros().toPlainString())
                                .build())
                        .collect(Collectors.toList());
                response = QueryOrgBalanceSummaryInfoResponse.newBuilder().addAllBalanceSummaryInfo(tokenHoldInfoList).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryOrgBalanceSummaryInfoResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryOrgBalanceSummaryInfo error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryTokenHoldInfo(QueryTokenHoldInfoRequest request, StreamObserver<QueryTokenHoldInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            Header header = request.getHeader();
            QueryTokenHoldInfoResponse response;
            try {
                List<StatisticsBalance> balanceList = orgStatisticsService.queryTokenHoldInfo(header.getOrgId(), header.getUserId(), request.getTokenId(),
                        request.getFromId(), request.getLastId(), request.getLimit());
                List<QueryTokenHoldInfoResponse.TokenHoldInfo> tokenHoldInfoList = balanceList.stream()
                        .map(balance -> QueryTokenHoldInfoResponse.TokenHoldInfo.newBuilder()
                                .setBalanceId(balance.getBalanceId())
                                .setUserId(balance.getUserId() == null ? 0 : balance.getUserId())
                                .setAccountId(balance.getAccountId())
                                .setTotal(balance.getTotal().stripTrailingZeros().toPlainString())
                                .setAvailable(balance.getAvailable().stripTrailingZeros().toPlainString())
                                .setLock(balance.getLocked().stripTrailingZeros().toPlainString())
                                .setPosition(balance.getPosition().stripTrailingZeros().toPlainString())
                                .build())
                        .collect(Collectors.toList());
                response = QueryTokenHoldInfoResponse.newBuilder().addAllTokenHoldInfo(tokenHoldInfoList).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryTokenHoldInfoResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryTokenHoldInfo error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryTokenHoldTopInfo(QueryTokenHoldTopInfoRequest request, StreamObserver<QueryTokenHoldTopInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            Header header = request.getHeader();
            QueryTokenHoldTopInfoResponse response;
            try {
                List<StatisticsBalance> balanceHoldTopList = orgStatisticsService.queryTokenHoldTopInfo(header.getOrgId(), request.getTokenId(), request.getTop());
                List<QueryTokenHoldTopInfoResponse.TokenHoldInfo> tokenHoldTopInfoList = balanceHoldTopList.stream()
                        .map(balance -> QueryTokenHoldTopInfoResponse.TokenHoldInfo.newBuilder()
                                .setBalanceId(balance.getBalanceId())
                                .setUserId(balance.getUserId() == null ? 0 : balance.getUserId())
                                .setAccountId(balance.getAccountId())
                                .setTotal(balance.getTotal().stripTrailingZeros().toPlainString())
                                .setAvailable(balance.getAvailable().stripTrailingZeros().toPlainString())
                                .setLock(balance.getLocked().stripTrailingZeros().toPlainString())
                                .setPosition(balance.getPosition().stripTrailingZeros().toPlainString())
                                .build())
                        .collect(Collectors.toList());
                response = QueryTokenHoldTopInfoResponse.newBuilder().addAllTokenHoldInfo(tokenHoldTopInfoList).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryTokenHoldTopInfoResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryTokenHoldTopInfo error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void adminQueryTokenHoldTopInfo(QueryTokenHoldTopInfoRequest request, StreamObserver<AdminQueryTokenHoldTopInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            Header header = request.getHeader();
            AdminQueryTokenHoldTopInfoResponse response;
            try {
                List<StatisticsBalance> balanceHoldTopList = orgStatisticsService.adminQueryTokenHoldTopInfo(header.getOrgId(), request.getTokenId(), header.getUserId(), request.getTop());
                List<AdminQueryTokenHoldTopInfoResponse.TokenHoldInfo> tokenHoldTopInfoList = balanceHoldTopList.stream()
                        .map(balance -> AdminQueryTokenHoldTopInfoResponse.TokenHoldInfo.newBuilder()
                                .setBalanceId(balance.getBalanceId())
                                .setUserId(balance.getUserId() == null ? 0 : balance.getUserId())
                                .setTokenId(balance.getTokenId())
                                .setTotal(balance.getTotal().stripTrailingZeros().toPlainString())
                                .build())
                        .collect(Collectors.toList());
                response = AdminQueryTokenHoldTopInfoResponse.newBuilder().addAllTokenHoldInfo(tokenHoldTopInfoList).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = AdminQueryTokenHoldTopInfoResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("adminQueryTokenHoldTopInfo error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void statisticsSymbolTradeFee(StatisticsSymbolTradeFeeRequest request, StreamObserver<StatisticsSymbolTradeFeeResponse> observer) {
        CompletableFuture.runAsync(() -> {
            Header header = request.getHeader();
            StatisticsSymbolTradeFeeResponse response;
            try {
                List<StatisticsSymbolTradeFee> tradeFeeList = orgStatisticsService.statisticsSymbolTradeFee(header.getOrgId(), header.getUserId(), request.getSymbolId(),
                        request.getStartTime(), request.getEndTime());
                List<StatisticsSymbolTradeFeeResponse.SymbolTradeFee> statisticsTradeFeeList = tradeFeeList.stream()
                        .map(tradeFee -> StatisticsSymbolTradeFeeResponse.SymbolTradeFee.newBuilder()
                                .setSymbolId(tradeFee.getSymbolId())
                                .setFeeTokenId(tradeFee.getFeeTokenId())
                                .setFeeTotal(tradeFee.getFeeTotal().stripTrailingZeros().toPlainString())
                                .build())
                        .collect(Collectors.toList());
                response = StatisticsSymbolTradeFeeResponse.newBuilder()
                        .addAllTradeFee(statisticsTradeFeeList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = StatisticsSymbolTradeFeeResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("statisticsSymbolTradeFee error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void statisticsSymbolTradeFeeTop(StatisticsSymbolTradeFeeTopRequest request, StreamObserver<StatisticsSymbolTradeFeeTopResponse> observer) {
        CompletableFuture.runAsync(() -> {
            Header header = request.getHeader();
            StatisticsSymbolTradeFeeTopResponse response;
            try {
                List<StatisticsTradeFeeTop> tradeFeeList = orgStatisticsService.statisticsTradeFeeTop(header.getOrgId(), request.getSymbolId(), request.getFeeTokenId(),
                        request.getStartTime(), request.getEndTime(), request.getTop());
                List<StatisticsSymbolTradeFeeTopResponse.SymbolTradeFeeTop> tradeFeeTopList = tradeFeeList.stream()
                        .map(tradeFeeTop -> StatisticsSymbolTradeFeeTopResponse.SymbolTradeFeeTop.newBuilder()
                                .setUserId(tradeFeeTop.getUserId() == null ? 0 : tradeFeeTop.getUserId())
                                .setAccountId(tradeFeeTop.getAccountId())
                                .setFeeTotal(tradeFeeTop.getFeeTotal().stripTrailingZeros().toPlainString())
                                .build())
                        .collect(Collectors.toList());
                response = StatisticsSymbolTradeFeeTopResponse.newBuilder()
                        .addAllTradeFeeTop(tradeFeeTopList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = StatisticsSymbolTradeFeeTopResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("statisticsSymbolTradeFeeTop error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryOrgDepositOrder(QueryOrgDepositOrderRequest request, StreamObserver<QueryOrgDepositOrderResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryOrgDepositOrderResponse> observer = (ServerCallStreamObserver<QueryOrgDepositOrderResponse>) responseObserver;
            observer.setCompression("gzip");
            Header header = request.getHeader();
            QueryOrgDepositOrderResponse response;
            try {
                List<StatisticsDepositOrder> depositOrderList = orgStatisticsService.queryOrgDepositOrder(header.getOrgId(), header.getUserId(), request.getTokenId(),
                        request.getStartTime(), request.getEndTime(), request.getFromId(), request.getLastId(), request.getLimit(),
                        request.getAddress(), request.getTxId());
                List<QueryOrgDepositOrderResponse.OrgDepositOrder> orgDepositOrderList = depositOrderList.stream()
                        .map(depositOrder -> QueryOrgDepositOrderResponse.OrgDepositOrder.newBuilder()
                                .setOrderId(depositOrder.getOrderId())
                                .setUserId(depositOrder.getUserId() == null ? 0 : depositOrder.getUserId())
                                .setAccountId(depositOrder.getAccountId())
                                .setTokenId(depositOrder.getTokenId())
                                .setQuantity(depositOrder.getQuantity().stripTrailingZeros().toPlainString())
                                .setFromAddress(Strings.nullToEmpty(depositOrder.getFromAddress()))
                                .setAddress(Strings.nullToEmpty(depositOrder.getWalletAddress()))
                                .setAddressTag(Strings.nullToEmpty(depositOrder.getWalletAddressTag()))
                                .setTxid(Strings.nullToEmpty(depositOrder.getTxId()))
                                .setTxidUrl(Strings.nullToEmpty(depositOrder.getTxIdUrl()))
                                .setStatus(depositOrder.getStatus())
                                .setCreateDate(depositOrder.getCreatedAt().toString())
                                .setCreateTime(depositOrder.getCreatedAt().getTime())
                                .setUpdateDate(depositOrder.getUpdatedAt().toString())
                                .setUpdateTime(depositOrder.getUpdatedAt().getTime())
                                .setReceiptType(depositOrder.getDepositReceiptType())
                                .setCannotReceiptReason(depositOrder.getCannotReceiptReason())
                                .build())
                        .collect(Collectors.toList());
                response = QueryOrgDepositOrderResponse.newBuilder()
                        .addAllDepositOrder(orgDepositOrderList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryOrgDepositOrderResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryOrgDepositOrder error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryOrgWithdrawOrder(QueryOrgWithdrawOrderRequest request, StreamObserver<QueryOrgWithdrawOrderResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryOrgWithdrawOrderResponse> observer = (ServerCallStreamObserver<QueryOrgWithdrawOrderResponse>) responseObserver;
            observer.setCompression("gzip");
            Header header = request.getHeader();
            QueryOrgWithdrawOrderResponse response;
            try {
                List<StatisticsWithdrawOrder> withdrawOrderList = orgStatisticsService.queryOrgWithdrawOrder(header.getOrgId(), header.getUserId(), request.getTokenId(),
                        request.getStartTime(), request.getEndTime(), request.getFromId(), request.getLastId(), request.getLimit(),
                        request.getAddress(), request.getTxId());
                List<QueryOrgWithdrawOrderResponse.OrgWithdrawOrder> orgWithdrawOrderList = withdrawOrderList.stream()
                        .map(withdrawOrder -> QueryOrgWithdrawOrderResponse.OrgWithdrawOrder.newBuilder()
                                .setOrderId(withdrawOrder.getOrderId())
                                .setClientWithdrawId(withdrawOrder.getClientWithdrawalId())
                                .setUserId(withdrawOrder.getUserId() == null ? 0 : withdrawOrder.getUserId())
                                .setAccountId(withdrawOrder.getAccountId())
                                .setTokenId(withdrawOrder.getTokenId())
                                .setTotalQuantity(withdrawOrder.getTotalQuantity().stripTrailingZeros().toPlainString())
                                .setArriveQuantity(withdrawOrder.getArriveQuantity().stripTrailingZeros().toPlainString())
                                .setAddress(Strings.nullToEmpty(withdrawOrder.getAddress()))
                                .setAddressTag(Strings.nullToEmpty(withdrawOrder.getAddressTag()))
                                .setTxid(Strings.nullToEmpty(withdrawOrder.getTxId()))
                                .setTxidUrl(Strings.nullToEmpty(withdrawOrder.getTxIdUrl()))
                                .setStatus(withdrawOrder.getStatus())
                                .setCreateDate(withdrawOrder.getCreatedAt().toString())
                                .setCreateTime(withdrawOrder.getCreatedAt().getTime())
                                .setUpdateDate(withdrawOrder.getUpdatedAt().toString())
                                .setUpdateTime(withdrawOrder.getUpdatedAt().getTime())
                                .setWalletHandleTime(withdrawOrder.getWalletHandleTime() == null ? 0L : withdrawOrder.getWalletHandleTime())
                                .build())
                        .collect(Collectors.toList());
                response = QueryOrgWithdrawOrderResponse.newBuilder()
                        .addAllWithdrawOrder(orgWithdrawOrderList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryOrgWithdrawOrderResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryOrgWithdrawOrder error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryOrgOTCOrder(QueryOrgOTCOrderRequest request, StreamObserver<QueryOrgOTCOrderResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryOrgOTCOrderResponse> observer = (ServerCallStreamObserver<QueryOrgOTCOrderResponse>) responseObserver;
            observer.setCompression("gzip");
            Header header = request.getHeader();
            QueryOrgOTCOrderResponse response;
            try {
                List<StatisticsOTCOrder> otcOrderList = orgStatisticsService.queryOrgOTCOrder(header.getOrgId(), header.getUserId(), request.getTokenId(),
                        request.getStartTime(), request.getEndTime(), request.getFromId(), request.getLastId(), request.getLimit());
                List<QueryOrgOTCOrderResponse.OrgOTCOrder> orgOTCOrderList = otcOrderList.stream()
                        .map(otcOrder -> QueryOrgOTCOrderResponse.OrgOTCOrder.newBuilder()
                                .setOrderId(otcOrder.getOrderId())
                                .setUserId(otcOrder.getUserId())
                                .setAccountId(otcOrder.getAccountId())
                                .setSide(otcOrder.getSide())
                                .setTokenId(otcOrder.getTokenId())
                                .setCurrencyId(otcOrder.getCurrencyId())
                                .setPrice(otcOrder.getPrice().stripTrailingZeros().toPlainString())
                                .setQuantity(otcOrder.getQuantity().stripTrailingZeros().toPlainString())
                                .setAmount(otcOrder.getAmount().stripTrailingZeros().toPlainString())
                                .setFee(otcOrder.getFee().stripTrailingZeros().toPlainString())
                                .setPaymentType(otcOrder.getPaymentType() == null ? -1 : otcOrder.getPaymentType())
                                .setStatus(otcOrder.getStatus())
                                .setTransferDate(otcOrder.getTransferDate() == null ? "" : otcOrder.getTransferDate().toString())
                                .setTransferTime(otcOrder.getTransferDate() == null ? 0 : otcOrder.getTransferDate().getTime())
                                .setCreateDate(otcOrder.getCreateDate().toString())
                                .setCreateTime(otcOrder.getCreateDate().getTime())
                                .setUpdateDate(otcOrder.getUpdateDate().toString())
                                .setUpdateTime(otcOrder.getUpdateDate().getTime())
                                .build())
                        .collect(Collectors.toList());
                response = QueryOrgOTCOrderResponse.newBuilder()
                        .addAllOtcOrder(orgOTCOrderList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryOrgOTCOrderResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryOrgOTCOrder error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryOrgTradeDetail(QueryOrgTradeDetailRequest request, StreamObserver<QueryOrgTradeDetailResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryOrgTradeDetailResponse> observer = (ServerCallStreamObserver<QueryOrgTradeDetailResponse>) responseObserver;
            observer.setCompression("gzip");
            Header header = request.getHeader();
            QueryOrgTradeDetailResponse response;
            try {
//                List<StatisticsTradeDetail> otcOrderList = orgStatisticsService.queryOrgTradeDetail(header.getOrgId(), request.getSymbolId(),
//                        request.getStartTime(), request.getEndTime(), request.getFromId(), request.getLastId(), request.getLimit());
//                List<QueryOrgTradeDetailResponse.TradeDetail> tradeDetailList = otcOrderList.stream()
//                        .map(tradeDetail -> QueryOrgTradeDetailResponse.TradeDetail.newBuilder()
//                                .setTradeId(tradeDetail.getTradeId())
//                                .setOrderId(tradeDetail.getOrderId())
//                                .setUserId(tradeDetail.getUserId() == null ? 0 : tradeDetail.getUserId())
//                                .setAccountId(tradeDetail.getAccountId())
//                                .setSymbolId(tradeDetail.getSymbolId())
//                                .setOrderType(tradeDetail.getOrderType())
//                                .setSide(tradeDetail.getSide())
//                                .setPrice(tradeDetail.getPrice().stripTrailingZeros().toPlainString())
//                                .setQuantity(tradeDetail.getQuantity().stripTrailingZeros().toPlainString())
//                                .setAmount(tradeDetail.getAmount().stripTrailingZeros().toPlainString())
//                                .setFeeToken(tradeDetail.getFeeToken())
//                                .setFee(tradeDetail.getFee().stripTrailingZeros().toPlainString())
//                                .setMatchOrgId(tradeDetail.getMatchOrgId())
//                                .setMatchUserId(tradeDetail.getMatchUserId())
//                                .setMatchAccountId(tradeDetail.getMatchAccountId())
//                                .setMatchTime(tradeDetail.getMatchTime().getTime())
//                                .setMatchDate(tradeDetail.getMatchTime().toString())
//                                .setCreateTime(tradeDetail.getCreatedAt().getTime())
//                                .setCreateDate(tradeDetail.getCreatedAt().toString())
//                                .setUpdateTime(tradeDetail.getUpdatedAt().getTime())
//                                .setUpdateDate(tradeDetail.getUpdatedAt().toString())
//                                .build())
//                        .collect(Collectors.toList());
                List<TradeDetail> otcOrderList = orgStatisticsService.queryEsTradeDetail(header, request.getAccountId(), request.getQueryDataTypeValue(), request.getOrderId(),
                        request.getSymbolId(), request.getStartTime(), request.getEndTime(), request.getFromId(), request.getLastId(), request.getLimit());
                List<QueryOrgTradeDetailResponse.TradeDetail> tradeDetailList = otcOrderList.stream()
                        .map(tradeDetail ->
                        {
                            OrderType orderType;
                            switch (tradeDetail.getOrderType()) {
                                case 4:
                                    orderType = OrderType.LIMIT_MAKER;
                                    break;
                                case 0:
                                    orderType = OrderType.LIMIT;
                                    break;
                                case 8:
                                    orderType = OrderType.LIMIT_FREE;
                                    break;
                                case 9:
                                    orderType = OrderType.LIMIT_MAKER_FREE;
                                    break;
                                default:
                                    orderType = OrderType.MARKET;
                                    break;
                            }

                            QueryOrgTradeDetailResponse.TradeDetail.Builder builder = QueryOrgTradeDetailResponse.TradeDetail.newBuilder();
                            builder.setTradeId(tradeDetail.getTradeDetailId())
                                    .setOrderId(tradeDetail.getOrderId())
                                    .setUserId(tradeDetail.getUserId() == null ? 0 : tradeDetail.getUserId())
                                    .setAccountId(tradeDetail.getAccountId())
                                    .setSymbolId(tradeDetail.getSymbolId())
                                    .setBaseTokenId(tradeDetail.getBaseTokenId())
                                    .setQuoteTokenId(tradeDetail.getQuoteTokenId())
                                    .setOrderType(orderType.getNumber())
                                    .setAmount(tradeDetail.getAmount().stripTrailingZeros().toPlainString())
                                    .setFeeToken(tradeDetail.getFeeToken())
                                    .setFeeRate(tradeDetail.getFeeRate().stripTrailingZeros().toPlainString())
                                    .setMatchOrgId(tradeDetail.getMatchOrgId() != null
                                            && tradeDetail.getOrgId().equals(tradeDetail.getMatchOrgId()) ? tradeDetail.getMatchOrgId() : 0)
                                    .setMatchUserId(tradeDetail.getMatchOrgId() != null && tradeDetail.getMatchUserId() != null
                                            && tradeDetail.getOrgId().equals(tradeDetail.getMatchOrgId()) ? tradeDetail.getMatchUserId() : 0)
                                    .setMatchAccountId(tradeDetail.getMatchAccountId() != null ? tradeDetail.getMatchAccountId() : 0)
//                                    .setMatchTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(tradeDetail.getMatchTime(), new ParsePosition(1)).getTime())
                                    .setMatchTime(tradeDetail.getMatchTime().getTime())
                                    .setMatchDate(tradeDetail.getMatchTime().toString())
//                                    .setCreateTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(tradeDetail.getCreatedAt(), new ParsePosition(1)).getTime())
                                    .setCreateTime(tradeDetail.getCreatedAt().getTime())
                                    .setCreateDate(tradeDetail.getCreatedAt().toString())
//                                    .setUpdateTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(tradeDetail.getUpdatedAt(), new ParsePosition(1)).getTime())
                                    .setUpdateTime(tradeDetail.getUpdatedAt().getTime())
                                    .setUpdateDate(tradeDetail.getUpdatedAt().toString())
                                    .setIsMaker(tradeDetail.getIsMaker())
                                    .setIsClose(tradeDetail.getIsClose())
                                    .setIsFuturesTrade(tradeDetail.getIsFuturesTrade() != null && tradeDetail.getIsFuturesTrade());
                            boolean isFuturesTrade = tradeDetail.getIsFuturesTrade() != null && tradeDetail.getIsFuturesTrade();
                            if (!isFuturesTrade) {
                                builder.setSide(tradeDetail.getOrderSide())
                                        .setOrderSide(OrderSideEnum.forNumber(tradeDetail.getOrderSide()).name())
                                        .setPrice(tradeDetail.getPrice().stripTrailingZeros().toPlainString())
                                        .setQuantity(tradeDetail.getQuantity().stripTrailingZeros().toPlainString())
                                        .setFee(tradeDetail.getFee().stripTrailingZeros().toPlainString());
//                                        .setPnl("0") // 盈亏的数值精度使用保证金的精度
//                                        .setLiquidationType(liquidationType.getNumber());
                            } else {
                                String symbolId = tradeDetail.getSymbolId();
                                String key = String.format("%s_%s", header.getOrgId(), symbolId);
                                Long exchangeId = basicService.getOrgExchangeFuturesMap().get(key);
                                if (exchangeId == null) {
                                    log.warn("getOrgExchangeFuturesMap null. key:{}", key);
                                    return null;
                                }
                                SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(exchangeId, symbolId);
                                if (symbolDetail == null) {
                                    log.warn("getSymbolDetailFutures null. exchangeId:{}, symbolId:{}", exchangeId, symbolId);
                                    return null;
                                }

                                // 手续费的显示精度使用保证金的精度
                                int marginPrecision = DecimalUtil.toBigDecimal(symbolDetail.getMarginPrecision()).stripTrailingZeros().scale();
                                DecimalStringFormatter feeFormatter = new DecimalStringFormatter(marginPrecision, BigDecimal.ROUND_DOWN, true);

                                int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
                                int basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();

                                // 根据extra_info获取强平类型
                                Order.LiquidationType liquidationType = Order.LiquidationType.NO_LIQ;
                                ExtraFlag extraFlag = ExtraFlag.forNumber(tradeDetail.getExtraInfo());
                                if (extraFlag == null) {
                                    log.error(String.format("Unknow extraFlag. trade: %s", JsonUtil.defaultGson().toJson(tradeDetail)));
                                } else {
                                    switch (extraFlag) {
                                        case LIQUIDATION_TRADE:
                                            liquidationType = Order.LiquidationType.IOC;
                                            break;
                                        case LIQUIDATION_ADL:
                                            liquidationType = Order.LiquidationType.ADL;
                                            break;
                                    }
                                }
                                //如果是反向转换方向
                                OrderSideEnum orderSide = symbolDetail.getIsReverse() ?
                                        FuturesUtil.getReverseOrderSideEnum(OrderSideEnum.forNumber(tradeDetail.getOrderSide())) :
                                        OrderSideEnum.forNumber(tradeDetail.getOrderSide());
                                builder.setSide(orderSide.getNumber())
                                        .setOrderSide(orderSide.name())
                                        .setContractMultiplier(tradeDetail.getContractMultiplier().stripTrailingZeros().toPlainString())
                                        .setFuturesPriceType(futuresOrderService.getPriceType(NewOrderRequest.ExtraFlagEnum.forNumber(tradeDetail.getExtraInfo())).getNumber())
                                        .setPrice(symbolDetail.getIsReverse() && tradeDetail.getPrice().compareTo(BigDecimal.ZERO) != 0 ?
                                                BigDecimal.ONE.divide(tradeDetail.getPrice(), minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString() :
                                                tradeDetail.getPrice().setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                                        .setQuantity(tradeDetail.getQuantity().setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                                        .setFee(feeFormatter.format(tradeDetail.getFee()))
                                        .setPnl(tradeDetail.getTradeDetailFutures() == null ? "0" :
                                                feeFormatter.format(tradeDetail.getTradeDetailFutures().getPnl().add(tradeDetail.getTradeDetailFutures().getResidual().negate()))) // 盈亏的数值精度使用保证金的精度
                                        .setLiquidationType(liquidationType.getNumber());
                            }
                            return builder.build();
                        })
                        .filter(item -> {
                            return item != null;
                        })
                        .collect(Collectors.toList());
//                if (request.getFromId() == 0 && request.getLastId() > 0) {
//                    tradeDetailList = Lists.reverse(tradeDetailList);
//                }
                response = QueryOrgTradeDetailResponse.newBuilder()
                        .addAllTradeDetail(tradeDetailList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryOrgTradeDetailResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryOrgTradeDetail error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryOrgBalanceFlow(QueryOrgBalanceFlowRequest request, StreamObserver<QueryOrgBalanceFlowResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryOrgBalanceFlowResponse> observer = (ServerCallStreamObserver<QueryOrgBalanceFlowResponse>) responseObserver;
            observer.setCompression("gzip");
            Header header = request.getHeader();
            QueryOrgBalanceFlowResponse response;
            try {
                List<BalanceFlow> esBalanceFlowList = orgStatisticsService.queryEsBalanceFlow(header, request.getAccountId(), request.getTokenId(), request.getBusinessSubjectList(),
                        request.getStartTime(), request.getEndTime(), request.getFromId(), request.getLastId(), request.getLimit());
                List<QueryOrgBalanceFlowResponse.BalanceFlow> balanceFlowList = esBalanceFlowList.stream()
                        .map(flow -> QueryOrgBalanceFlowResponse.BalanceFlow.newBuilder()
                                .setOrgId(flow.getOrgId())
                                .setBalanceFlowId(flow.getBalanceFlowId())
                                .setUserId(Strings.nullToEmpty(flow.getUserId().toString()))
                                .setAccountId(flow.getAccountId())
                                .setTokenId(flow.getTokenId())
                                .setBusinessSubject(flow.getBusinessSubject())
                                .setSecondBusinessSubject(flow.getSecondBusinessSubject())
                                .setChanged(flow.getChanged().stripTrailingZeros().toPlainString())
                                .setTotal(flow.getTotal().stripTrailingZeros().toPlainString())
//                                .setCreatedTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(flow.getCreatedAt(), new ParsePosition(1)).getTime())
                                .setCreatedTime(flow.getCreatedAt().getTime())
                                .build())
                        .collect(Collectors.toList());
                response = QueryOrgBalanceFlowResponse.newBuilder()
                        .addAllBalanceFlow(balanceFlowList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryOrgBalanceFlowResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryOrgBalanceFlow error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryBalanceLockRecordList(QueryBalanceLockRecordRequest request, StreamObserver<QueryBalanceLockRecordResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryBalanceLockRecordResponse> observer = (ServerCallStreamObserver<QueryBalanceLockRecordResponse>) responseObserver;
            observer.setCompression("gzip");
            Header header = request.getHeader();
            QueryBalanceLockRecordResponse response;
            try {
                List<StatisticsBalanceLockRecord> balanceLockRecords = orgStatisticsService.queryBalanceLockRecordList(header.getOrgId(), header.getUserId(), request.getTokenId(),
                        request.getBusinessSubject(), request.getSecondBusinessSubject(), request.getStatus(), request.getFromId(), request.getLastId(), request.getLimit());
                List<QueryBalanceLockRecordResponse.BalanceLockRecord> recordList = balanceLockRecords.stream()
                        .map(lockRecord -> QueryBalanceLockRecordResponse.BalanceLockRecord.newBuilder()
                                .setId(lockRecord.getId())
                                .setAccountId(lockRecord.getAccountId())
                                .setBalanceId(lockRecord.getBalanceId())
                                .setBusinessSubject(lockRecord.getBusinessSubject())
                                .setSecondBusinessSubject(lockRecord.getSecondBusinessSubject())
                                .setClientReqId(lockRecord.getClientReqId())
                                .setCreateDate(lockRecord.getCreatedAt().toString())
                                .setCreateTime(lockRecord.getCreatedAt().getTime())
                                .setLockAmount(lockRecord.getLockAmount().stripTrailingZeros().toPlainString())
                                .setNewLockedValue(lockRecord.getNewLockedValue().stripTrailingZeros().toPlainString())
                                .setLockReason(lockRecord.getLockReason())
                                .setOrgId(lockRecord.getOrgId())
                                .setRecordId(lockRecord.getRecordId())
                                .setStatus(lockRecord.getStatus())
                                .setSubjectExtId(lockRecord.getSubjectExtId())
                                .setTokenId(lockRecord.getTokenId())
                                .setUnlockedValue(lockRecord.getUnlockedValue().stripTrailingZeros().toPlainString())
                                .setUpdateDate(lockRecord.getUpdatedAt().toString())
                                .setUpdateTime(lockRecord.getUpdatedAt().getTime())
                                .setUserId(header.getUserId())
                                .build())
                        .collect(Collectors.toList());
                response = QueryBalanceLockRecordResponse.newBuilder()
                        .addAllLockRecord(recordList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryBalanceLockRecordResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryBalanceLockRecordList error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }

    @Override
    public void queryBalanceUnlockRecordList(QueryBalanceUnlockRecordRequest request, StreamObserver<QueryBalanceUnlockRecordResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryBalanceUnlockRecordResponse> observer = (ServerCallStreamObserver<QueryBalanceUnlockRecordResponse>) responseObserver;
            observer.setCompression("gzip");
            Header header = request.getHeader();
            QueryBalanceUnlockRecordResponse response;
            try {
                List<StatisticsBalanceUnlockRecord> unlockRecords = orgStatisticsService.queryBalanceUnlockRecordList(header.getOrgId(), header.getUserId(), request.getTokenId(),
                        request.getBusinessSubject(), request.getSecondBusinessSubject(), request.getFromId(), request.getLastId(), request.getLimit());
                List<QueryBalanceUnlockRecordResponse.BalanceUnlockRecord> recordList = unlockRecords.stream()
                        .map(lockRecord -> QueryBalanceUnlockRecordResponse.BalanceUnlockRecord.newBuilder()
                                .setId(lockRecord.getId())
                                .setAccountId(lockRecord.getAccountId())
                                .setBalanceId(lockRecord.getBalanceId())
                                .setBusinessSubject(lockRecord.getBusinessSubject())
                                .setSecondBusinessSubject(lockRecord.getSecondBusinessSubject())
                                .setClientReqId(lockRecord.getClientReqId())
                                .setUnlockAmount(lockRecord.getUnlockAmount().stripTrailingZeros().toPlainString())
                                .setNewLockedValue(lockRecord.getNewLockedValue().stripTrailingZeros().toPlainString())
                                .setUnlockReason(lockRecord.getUnlockReason())
                                .setOrgId(lockRecord.getOrgId())
                                .setClientReqId(lockRecord.getClientReqId())
                                .setOriginClientReqId(lockRecord.getOriginClientReqId())
                                .setLockRecordId(lockRecord.getLockRecordId())
                                .setSubjectExtId(lockRecord.getSubjectExtId())
                                .setTokenId(lockRecord.getTokenId())
                                .setCreateDate(lockRecord.getCreatedAt().toString())
                                .setCreateTime(lockRecord.getCreatedAt().getTime())
                                .setUserId(header.getUserId())
                                .build())
                        .collect(Collectors.toList());
                response = QueryBalanceUnlockRecordResponse.newBuilder()
                        .addAllUnlockRecord(recordList)
                        .build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
                response = QueryBalanceUnlockRecordResponse.newBuilder().setBasicRet(basicRet).setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryBalanceLockRecordList error, request:{}", JsonUtil.defaultGson().toJson(request), e);
                observer.onError(e);
            }
        }, taskExecutor);
    }
}
