package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.otc.third.party.*;
import io.bhex.broker.server.grpc.server.service.OtcThirdPartyService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author cookie.yuan
 * @description
 * @date 2020-09-16
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcThirdPartyGrpcService extends OtcThirdPartyServiceGrpc.OtcThirdPartyServiceImplBase {

    @Resource
    OtcThirdPartyService otcThirdPartyService;

    public void getOtcThirdPartyConfig(GetOtcThirdPartyConfigRequest request, StreamObserver<GetOtcThirdPartyConfigResponse> responseObserver) {
        GetOtcThirdPartyConfigResponse response;
        try {
            response = otcThirdPartyService.getOtcThirdPartyConfig(request.getHeader().getOrgId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOtcThirdPartyConfigResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOtcThirdPartyConfig exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void getOtcThirdParty(GetOtcThirdPartyRequest request, StreamObserver<GetOtcThirdPartyResponse> responseObserver) {
        GetOtcThirdPartyResponse response;
        try {
            List<OtcThirdParty> thirdPartyList = otcThirdPartyService.getOtcThirdParty(request.getHeader().getOrgId(),request.getSide());
            response = GetOtcThirdPartyResponse.newBuilder()
                    .addAllThirdParty(thirdPartyList)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOtcThirdPartyResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOtcThirdParty exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void getOtcThirdPartySymbols(GetOtcThirdPartySymbolsRequest request, StreamObserver<GetOtcThirdPartySymbolsResponse> responseObserver) {
        GetOtcThirdPartySymbolsResponse response;
        try {
            List<OtcThirdPartySymbol> symbolList = otcThirdPartyService.getOtcThirdPartySymbols(request.getHeader().getOrgId(),
                    request.getThirdPartyId(),request.getSide());
            response = GetOtcThirdPartySymbolsResponse.newBuilder()
                    .addAllTokenId(symbolList.stream().map(o -> o.getTokenId()).distinct().collect(Collectors.toList()))
                    .addAllCurrencyId(symbolList.stream().map(o -> o.getCurrencyId()).distinct().collect(Collectors.toList()))
                    .addAllSymbol(symbolList)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOtcThirdPartySymbolsResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOtcThirdPartySymbols exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void getOtcThirdPartyPayments(GetOtcThirdPartyPaymentsRequest request, StreamObserver<GetOtcThirdPartyPaymentsResponse> responseObserver) {
        GetOtcThirdPartyPaymentsResponse response;
        try {
            List<OtcThirdPartyPayment> paymentList = otcThirdPartyService.getOtcThirdPartyPayments(request.getHeader().getOrgId(),
                    request.getThirdPartyId(), request.getTokenId(), request.getCurrencyId(), request.getSide());
            response = GetOtcThirdPartyPaymentsResponse.newBuilder()
                    .addAllOtcThirdPartyPayment(paymentList)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOtcThirdPartyPaymentsResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOtcThirdPartyPayments exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void getOtcThirdPartyPrice(GetOtcThirdPartyPriceRequest request, StreamObserver<GetOtcThirdPartyPriceResponse> responseObserver) {
        GetOtcThirdPartyPriceResponse response;
        try {
            response = otcThirdPartyService.getOtcThirdPartyPrice(request.getHeader(), request.getOtcSymbolId(), request.getTokenAmount(),
                    request.getCurrencyAmount(), request.getSide());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOtcThirdPartyPriceResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getOtcThirdPartyPayments exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void createOtcThirdPartyOrder(CreateOtcThirdPartyOrderRequest request, StreamObserver<CreateOtcThirdPartyOrderResponse> responseObserver) {
        CreateOtcThirdPartyOrderResponse response;
        try {
            response = otcThirdPartyService.createOtcThirdPartyOrder(request.getHeader(),
                    request.getOtcSymbolId(),
                    request.getClientOrderId(),
                    request.getTokenAmount(),
                    request.getCurrencyAmount(),
                    request.getPrice(),
                    request.getSide());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = CreateOtcThirdPartyOrderResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("createOtcThirdPartyOrder exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void queryOtcThirdPartyOrders(QueryOtcThirdPartyOrdersRequest request, StreamObserver<QueryOtcThirdPartyOrdersResponse> responseObserver) {
        QueryOtcThirdPartyOrdersResponse response;
        try {
            List<OtcThirdPartyOrder> orderList = otcThirdPartyService.queryOtcThirdPartyOrder(request.getHeader().getOrgId(),
                    request.getHeader().getUserId(), request.getStatus(),
                    request.getBeginTime(), request.getEndTime(),
                    request.getFromOrderId(), request.getCount());
            response = QueryOtcThirdPartyOrdersResponse.newBuilder()
                    .addAllOtcThirdPartyOrder(orderList)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryOtcThirdPartyOrdersResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryOtcThirdPartyOrders exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void queryOtcThirdPartyDisclaimer(QueryOtcThirdPartyDisclaimerRequest request, StreamObserver<QueryOtcThirdPartyDisclaimerResponse> responseObserver) {
        QueryOtcThirdPartyDisclaimerResponse response;
        try {
            List<OtcThirdPartyDisclaimer> disclaimerList = otcThirdPartyService.queryOtcThirdPartyDisclaimer(
                    request.getHeader().getOrgId(), request.getThirdPartyId(), request.getLanguage());
            response = QueryOtcThirdPartyDisclaimerResponse.newBuilder()
                    .addAllThirdPartyDisclaimer(disclaimerList)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryOtcThirdPartyDisclaimerResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryOtcThirdPartyDisclaimer exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void updateOtcThirdPartyDisclaimer(UpdateOtcThirdPartyDisclaimerRequest request, StreamObserver<UpdateOtcThirdPartyDisclaimerResponse> responseObserver) {
        UpdateOtcThirdPartyDisclaimerResponse response;
        try {
            response = otcThirdPartyService.updateOtcThirdPartyDisclaimer(request.getHeader().getOrgId(),
                    request.getThirdPartyDisclaimerList());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = UpdateOtcThirdPartyDisclaimerResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("updateOtcThirdPartyDisclaimer exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void queryOtcThirdPartyOrdersByAdmin(QueryOtcThirdPartyOrdersByAdminRequest request, StreamObserver<QueryOtcThirdPartyOrdersResponse> responseObserver) {
        QueryOtcThirdPartyOrdersResponse response;
        try {
            List<OtcThirdPartyOrder> orderList = otcThirdPartyService.queryOtcThirdPartyOrderByAdmin(request.getHeader().getOrgId(),
                    request.getUserId(), request.getOrderId(), request.getStatus(),
                    request.getBeginTime(), request.getEndTime(),
                    request.getFromOrderId(), request.getCount());
            response = QueryOtcThirdPartyOrdersResponse.newBuilder()
                    .addAllOtcThirdPartyOrder(orderList)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryOtcThirdPartyOrdersResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryOtcThirdPartyOrders exception:{}", e);
            responseObserver.onError(e);
        }
    }

    public void moonpayTransaction(MoonpayTransactionRequest request, StreamObserver<MoonpayTransactionResponse> responseObserver) {
        MoonpayTransactionResponse response;
        try {
            response = otcThirdPartyService.moonpayTransaction(request.getTransactionId(), request.getTransactionStatus(),
                    request.getOrderId(),request.getFeeAmount(),request.getExtraFeeAmount(),request.getNetworkFeeAmount());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = MoonpayTransactionResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("moonpayTransaction exception:{}", e);
            responseObserver.onError(e);
        }
    }

}
