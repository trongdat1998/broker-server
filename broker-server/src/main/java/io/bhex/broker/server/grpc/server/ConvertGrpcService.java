package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.convert.*;
import io.bhex.broker.server.grpc.server.service.ConvertService;
import io.bhex.broker.server.model.ConvertSymbol;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author cookie.yuan
 * @description
 * @date 2020-08-14
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class ConvertGrpcService extends ConvertServiceGrpc.ConvertServiceImplBase {

    @Resource
    ConvertService convertService;

    public void getConvertSymbols(GetConvertSymbolsRequest request, StreamObserver<GetConvertSymbolsResponse> observer) {
        GetConvertSymbolsResponse response;
        try {
            List<ConvertSymbol> symbolList = convertService.getConvertSymbols(request.getHeader());
            List<String> offeringsTokens = convertService.queryOfferingsTokens(request.getHeader());
            List<String> purchaseTokens = convertService.queryPurchaseTokens(request.getHeader());
            response = GetConvertSymbolsResponse.newBuilder()
                    .addAllConvertSymbol(symbolList.stream().map(this::buildConvertSymbol).collect(Collectors.toList()))
                    .addAllOfferingsTokenId(offeringsTokens)
                    .addAllPurchaseTokenId(purchaseTokens)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetConvertSymbolsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getConvertSymbols exception:{}", e);
            observer.onError(e);
        }
    }

    private io.bhex.broker.grpc.convert.ConvertSymbol buildConvertSymbol(ConvertSymbol convertSymbol) {
        return io.bhex.broker.grpc.convert.ConvertSymbol.newBuilder()
                .setConvertSymbolId(convertSymbol.getId())
                .setBrokerId(convertSymbol.getBrokerId())
                .setBrokerAccountId(convertSymbol.getBrokerAccountId())
                .setSymbolId(convertSymbol.getSymbolId())
                .setPurchaseTokenId(convertSymbol.getPurchaseTokenId())
                .setPurchaseTokenName(convertSymbol.getPurchaseTokenName())
                .setOfferingsTokenId(convertSymbol.getOfferingsTokenId())
                .setOfferingsTokenName(convertSymbol.getOfferingsTokenName())
                .setPurchasePrecision(convertSymbol.getPurchasePrecision())
                .setOfferingsPrecision(convertSymbol.getOfferingsPrecision())
                .setPriceType(convertSymbol.getPriceType())
                .setPriceValue(DecimalUtil.toTrimString(convertSymbol.getPriceValue()))
                .setMinQuantity(DecimalUtil.toTrimString(convertSymbol.getMinQuantity()))
                .setMaxQuantity(DecimalUtil.toTrimString(convertSymbol.getMaxQuantity()))
                .setAccountDailyLimit(DecimalUtil.toTrimString(convertSymbol.getAccountDailyLimit()))
                .setAccountTotalLimit(DecimalUtil.toTrimString(convertSymbol.getAccountTotalLimit()))
                .setSymbolDailyLimit(DecimalUtil.toTrimString(convertSymbol.getSymbolDailyLimit()))
                .setStatus(convertSymbol.getStatus())
                .setVerifyKyc(convertSymbol.getVerifyKyc())
                .setVerifyMobile(convertSymbol.getVerifyMobile())
                .setVerifyVipLevel(convertSymbol.getVerifyVipLevel())
                .build();
    }

    public void getConvertPrice(GetConvertPriceRequest request, StreamObserver<GetConvertPriceResponse> observer) {
        GetConvertPriceResponse response;
        try {
            response = convertService.getConvertPrice(request.getHeader(), request.getConvertSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetConvertPriceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getConvertPrice exception:{}", e);
            observer.onError(e);
        }
    }

    public void createConvertOrder(CreateConvertOrderRequest request, StreamObserver<CreateConvertOrderResponse> observer) {
        CreateConvertOrderResponse response;
        try {
            response = convertService.createConvertOrder(request.getHeader(),
                    request.getConvertSymbolId(),
                    request.getClientOrderId(),
                    request.getPurchaseQuantity(),
                    request.getOfferingsQuantity(),
                    request.getPrice(),
                    request.getOrderTokenId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateConvertOrderResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createConvertOrder exception:{}", e);
            observer.onError(e);
        }
    }

    public void queryConvertOrders(QueryConvertOrdersRequest request, StreamObserver<QueryConvertOrdersResponse> observer) {
        QueryConvertOrdersResponse response;
        try {
            List<ConvertOrder> orderList = convertService.queryOrders(request.getHeader(),
                    request.getConvertSymbolId(), request.getStatus(),
                    request.getBeginTime(), request.getEndTime(),
                    request.getStartId(), request.getCount());
            response = QueryConvertOrdersResponse.newBuilder()
                    .addAllConvertOrder(orderList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryConvertOrdersResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryConvertOrders exception:{}", e);
            observer.onError(e);
        }
    }


    public void addConvertSymbol(AddConvertSymbolRequest request, StreamObserver<AddConvertSymbolResponse> observer) {
        AddConvertSymbolResponse response;
        try {
            response = convertService.addConvertSymbol(request.getHeader(),
                    request.getSymbolId(),
                    request.getBrokerAccountId(),
                    request.getPurchaseTokenId(),
                    request.getOfferingsTokenId(),
                    request.getPurchasePrecision(),
                    request.getOfferingsPrecision(),
                    request.getPriceType(),
                    request.getPriceValue(),
                    request.getMinQuantity(),
                    request.getMaxQuantity(),
                    request.getAccountDailyLimit(),
                    request.getAccountTotalLimit(),
                    request.getSymbolDailyLimit(),
                    request.getVerifyKyc(),
                    request.getVerifyMobile(),
                    request.getVerifyVipLevel(),
                    request.getStatus());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AddConvertSymbolResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("addConvertSymbol exception:{}", e);
            observer.onError(e);
        }
    }

    public void modifyConvertSymbol(ModifyConvertSymbolRequest request, StreamObserver<ModifyConvertSymbolResponse> observer) {
        ModifyConvertSymbolResponse response;
        try {
            response = convertService.modifyConvertSymbol(
                    request.getHeader(),
                    request.getConvertSymbolId(),
                    request.getSymbolId(),
                    request.getPurchaseTokenId(),
                    request.getOfferingsTokenId(),
                    request.getPurchasePrecision(),
                    request.getOfferingsPrecision(),
                    request.getBrokerAccountId(),
                    request.getPriceType(),
                    request.getPriceValue(),
                    request.getMinQuantity(),
                    request.getMaxQuantity(),
                    request.getAccountDailyLimit(),
                    request.getAccountTotalLimit(),
                    request.getSymbolDailyLimit(),
                    request.getVerifyKyc(),
                    request.getVerifyMobile(),
                    request.getVerifyVipLevel());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ModifyConvertSymbolResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("modifyConvertSymbol exception:{}", e);
            observer.onError(e);
        }

    }

    public void updateConvertSymbolStatus(UpdateConvertSymbolStatusRequest request, StreamObserver<UpdateConvertSymbolStatusResponse> observer) {
        UpdateConvertSymbolStatusResponse response;
        try {
            response = convertService.updateConvertSymbolStatus(request.getHeader(), request.getConvertSymbolId(), request.getStatus());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateConvertSymbolStatusResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("updateConvertSymbolStatus exception:{}", e);
            observer.onError(e);
        }
    }

    public void adminQueryConvertOrders(AdminQueryConvertOrdersRequest request, StreamObserver<AdminQueryConvertOrdersResponse> observer) {
        AdminQueryConvertOrdersResponse response;
        try {
            List<ConvertOrder> orderList = convertService.adminQueryOrders(request.getHeader(), request.getUserId(),
                    request.getConvertSymbolId(), request.getStatus(),
                    request.getBeginTime(), request.getEndTime(),
                    request.getStartId(), request.getCount());
            response = AdminQueryConvertOrdersResponse.newBuilder()
                    .addAllConvertOrder(orderList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AdminQueryConvertOrdersResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryConvertOrders exception:{}", e);
            observer.onError(e);
        }
    }


}
