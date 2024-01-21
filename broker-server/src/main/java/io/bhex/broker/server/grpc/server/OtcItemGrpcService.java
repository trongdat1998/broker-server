package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.grpc.server.service.OtcService;
import io.bhex.broker.server.util.OrgConfigUtil;
import io.bhex.ex.otc.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * 商品/广告grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-16
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcItemGrpcService extends OTCItemServiceGrpc.OTCItemServiceImplBase {

    @Autowired
    private GrpcOtcService grpcOtcService;

    @Autowired
    private OtcService otcService;

    @Override
    public void addItem(OTCNewItemRequest request, StreamObserver<OTCNewItemResponse> responseObserver) {

        try {
            responseObserver.onNext(otcService.createItem(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void cancelItemToDelete(OTCCancelItemRequest request,
                                   StreamObserver<OTCCancelItemResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.cancelItem(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getItems(OTCGetItemsRequest request, StreamObserver<OTCGetItemsResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getItemList(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOnlineItems(OTCGetOnlineItemsRequest request, StreamObserver<OTCGetItemsResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getItemList(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getItem(OTCGetItemInfoRequest request, StreamObserver<OTCGetItemInfoResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.getItemInfo(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getDepth(OTCGetDepthRequest request, StreamObserver<OTCGetDepthResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.getDepth(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void offlineItem(OTCOfflineItemRequest request, StreamObserver<OTCOfflineItemResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.offlineItem(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void onlineItem(OTCOnlineItemRequest request, StreamObserver<OTCOnlineItemResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.onlineItem(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getLastPrice(OTCGetLastPriceRequest request, StreamObserver<OTCGetLastPriceResponse> responseObserver) {
        try {
            responseObserver.onNext(grpcOtcService.getLastPrice(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getTradeFeeRate(GetTradeFeeRateRequest request, StreamObserver<GetTradeFeeRateResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getTradeFeeRate(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getTradeFeeRateByTokenId(GetTradeFeeRateByTokenIdRequest request, StreamObserver<GetTradeFeeRateByTokenIdResponse> responseObserver) {

        try {
            responseObserver.onNext(grpcOtcService.getTradeFeeRateByTokenId(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}
