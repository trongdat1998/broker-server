package io.bhex.broker.server.grpc.server;

import com.google.protobuf.TextFormat;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.finance.*;
import io.bhex.broker.server.grpc.server.service.*;
import io.bhex.broker.server.model.FinanceProduct;
import io.bhex.broker.server.model.FinanceRecord;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class FinanceGrpcService extends FinanceServiceGrpc.FinanceServiceImplBase {

    @Resource
    private FinanceProductService financeProductService;

    @Resource
    private FinanceWalletService financeWalletService;

    @Resource
    private FinanceRecordService financeRecordService;

    @Resource
    private FinancePurchaseService financePurchaseService;

    @Resource
    private FinanceRedeemService financeRedeemService;

    public void getFinanceProductList(GetFinanceProductListRequest request, StreamObserver<GetFinanceProductListResponse> observer) {
        GetFinanceProductListResponse response;
        try {
            List<FinanceProduct> productList = financeProductService.getFinanceProductByOrgId(request.getOrgId());
            response = GetFinanceProductListResponse.newBuilder()
                    .addAllProducts(financeProductService.packageProductList(productList))
                    .build();
        } catch (Exception e) {
            log.error(" getFinanceProductList exception:{}", TextFormat.shortDebugString(request), e);
            response = GetFinanceProductListResponse.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getFinanceWalletList(GetFinanceWalletListRequest request, StreamObserver<GetFinanceWalletListResponse> observer) {
        GetFinanceWalletListResponse response;
        try {
            response = financeWalletService.getFinanceWalletList(request.getOrgId(), request.getUserId());
        } catch (Exception e) {
            response = GetFinanceWalletListResponse.getDefaultInstance();
            log.error(" getFinanceWalletList exception:{} ", TextFormat.shortDebugString(request), e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getSingleFinanceWallet(GetSingleFinanceWalletRequest request, StreamObserver<GetSingleFinanceWalletResponse> observer) {
        GetSingleFinanceWalletResponse response;
        try {
            response = financeWalletService.getSingleFinanceWallet(request.getOrgId(), request.getUserId(), request.getProductId());
        } catch (Exception e) {
            response = GetSingleFinanceWalletResponse.getDefaultInstance();
            log.error(" getSingleFinanceWallet exception:{}", TextFormat.shortDebugString(request), e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getFinanceRecordList(GetFinanceRecordListRequest request, StreamObserver<GetFinanceRecordListResponse> observer) {
        GetFinanceRecordListResponse response;
        try {
            List<FinanceRecord> recordList = financeRecordService.getFinanceRecordList(request.getOrgId(), request.getUserId(), request.getStartRecordId(), request.getLimit());
            response = GetFinanceRecordListResponse.newBuilder()
                    .addAllRecords(financeRecordService.convertRecordList(recordList))
                    .build();
        } catch (Exception e) {
            response = GetFinanceRecordListResponse.getDefaultInstance();
            log.error(" getFinanceRecordList exception:{}", TextFormat.shortDebugString(request), e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getSingleFinanceRecord(GetSingleFinanceRecordRequest request, StreamObserver<GetSingleFinanceRecordResponse> observer) {
        GetSingleFinanceRecordResponse response;
        try {
            FinanceRecord record = financeRecordService.getSingleFinanceRecord(request.getOrgId(), request.getUserId(), request.getRecordId());
            response = GetSingleFinanceRecordResponse.newBuilder()
                    .setRecord(financeRecordService.convertRecord(record))
                    .build();
        } catch (BrokerException e) {
            response = GetSingleFinanceRecordResponse.newBuilder().setRet(e.getCode()).build();
        } catch (Exception e) {
            response = GetSingleFinanceRecordResponse.getDefaultInstance();
            log.error(" getSingleFinanceRecord exception:{}", TextFormat.shortDebugString(request), e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    public void purchaseFinance(PurchaseFinanceRequest request, StreamObserver<PurchaseFinanceResponse> observer) {
        PurchaseFinanceResponse response;
        try {
            response = financePurchaseService.purchase(request.getHeader(),
                    request.getOrgId(), request.getUserId(), request.getProductId(), new BigDecimal(request.getAmount()));
        } catch (BrokerException e) {
            response = PurchaseFinanceResponse.newBuilder().setRet(e.getCode()).build();
        } catch (Exception e) {
            log.error(" purchaseFinance exception:{}", TextFormat.shortDebugString(request), e);
            response = PurchaseFinanceResponse.newBuilder().setRet(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR.code()).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void redeemFinance(RedeemFinanceRequest request, StreamObserver<RedeemFinanceResponse> observer) {
        RedeemFinanceResponse response;
        try {
            response = financeRedeemService.redeem(request.getHeader(),
                    request.getOrgId(), request.getUserId(), request.getProductId(), new BigDecimal(request.getAmount()));
        } catch (BrokerException e) {
            response = RedeemFinanceResponse.newBuilder().setRet(e.getCode()).build();
        } catch (Exception e) {
            log.error(" redeemFinance exception:{}", TextFormat.shortDebugString(request), e);
            response = RedeemFinanceResponse.newBuilder().setRet(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR.code()).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

}
