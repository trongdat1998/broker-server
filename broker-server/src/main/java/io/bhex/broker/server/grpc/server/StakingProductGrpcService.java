package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.order.CreateOrderResponse;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.grpc.server.service.po.StakingRedeemResponseDTO;
import io.bhex.broker.server.grpc.server.service.po.StakingSubscribeResponseDTO;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductOrderService;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.math.BigDecimal;

@Slf4j
@GrpcService
public class StakingProductGrpcService extends StakingProductServiceGrpc.StakingProductServiceImplBase {

    @Autowired
    StakingProductService stakingProductService;

    @Resource
    private StakingProductOrderService stakingProductOrderService;

    @Override
    public void getProductList(GetProductListRequest request, StreamObserver<GetProductListReply> responseObserver) {
        try {
            GetProductListReply reply = stakingProductService.getProductList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getStakingAssetList(GetStakingAssetListRequest request, StreamObserver<GetStakingAssetListReply> responseObserver) {
        try {
            GetStakingAssetListReply reply = stakingProductService.getStakingAssetList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * 申购
     *
     * @param request
     * @param observer
     */
    @Override
    public void subscribeProduct(StakingSubscribeRequest request
            , StreamObserver<StakingSubscribeResponse> observer) {
        StakingSubscribeResponse response;
        try {
            StakingSubscribeResponseDTO responseDTO = stakingProductOrderService.subscribeProduct(request.getHeader()
                    , request.getTokenId()
                    , request.getProductId()
                    , request.getLots()
                    , new BigDecimal(request.getAmount())
                    , request.getStakingProductType());
            response = StakingSubscribeResponse.newBuilder().setTransferId(responseDTO.getTransferId()).setRet(responseDTO.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = StakingSubscribeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("staking product subscribe error", e);
            observer.onError(e);
        }
    }

//    /**
//     * 申购
//     *
//     * @param request
//     * @param observer
//     */
//    @Override
//    public void subscribeTimeProduct(StakingTimeSubscribeRequest request
//            , StreamObserver<StakingSubscribeResponse> observer) {
//        StakingSubscribeResponse response;
//        try {
//            StakingSubscribeResponseDTO responseDTO = stakingProductOrderService.subscribeTimeProduct(request.getHeader()
//                    , request.getTokenId()
//                    , request.getProductId()
//                    , new BigDecimal(request.getLots())
//                    , request.getCanAutoRenew());
//            response = StakingSubscribeResponse.newBuilder().setTransferId(responseDTO.getTransferId()).setRet(responseDTO.getCode()).build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (BrokerException e) {
//            response = StakingSubscribeResponse.newBuilder().setRet(e.getCode()).build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (Exception e) {
//            log.error("staking product subscribe error", e);
//            observer.onError(e);
//        }
//    }

    /**
     * 查询申购订单
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void getStakingProductOrder(GetStakingProductOrderRequest request
            , StreamObserver<GetStakingProductOrderResponse> responseObserver) {
        try {
            GetStakingProductOrderResponse response = stakingProductOrderService.getStakingProductOrder(request.getHeader()
                    , request.getProductId()
                    , request.getTransferId()
                    , request.getOrderId());

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * 查询申购结果
     *
     * @param request
     * @param observer
     */
    @Override
    public void subscribeResult(GetStakingSubscribeResultRequest request
            , StreamObserver<GetStakingSubscribeResultResponse> observer) {
        GetStakingSubscribeResultResponse response;
        try {
            Integer resultCode = stakingProductOrderService.getSubscribeResult(request.getHeader()
                    , request.getProductId()
                    , request.getTransferId());
            response = GetStakingSubscribeResultResponse.newBuilder().setRet(resultCode).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetStakingSubscribeResultResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("staking product get subscribe result error", e);
            observer.onError(e);
        }
    }

    /**
     * 赎回
     *
     * @param request
     * @param observer
     */
    @Override
    public void redeemAmount(StakingRedeemRequest request
            , StreamObserver<StakingRedeemResponse> observer) {
        StakingRedeemResponse response;
        try {
            StakingRedeemResponseDTO redeemResponseDTO = stakingProductOrderService.redeemAmount(request.getHeader()
                    , request.getProductId()
                    , request.getOrderId()
                    , new BigDecimal(request.getAmount())
                    , StakingProductType.FI_CURRENT);
            response = StakingRedeemResponse.newBuilder()
                    .setTransferId(redeemResponseDTO.getTransferId())
                    .setRet(redeemResponseDTO.getCode())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = StakingRedeemResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("staking product redeem error", e);
            observer.onError(e);
        }
    }

    /**
     * 查询赎回结果
     *
     * @param request
     * @param observer
     */
    @Override
    public void redeemAmountResult(GetRedeemAmountResultRequest request
            , StreamObserver<GetRedeemAmountResultResponse> observer) {
        GetRedeemAmountResultResponse response;
        try {
            Integer resultCode = stakingProductOrderService.redeemResult(request.getHeader()
                    , request.getProductId()
                    , request.getTransferId());
            response = GetRedeemAmountResultResponse.newBuilder().setRet(resultCode).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetRedeemAmountResultResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("staking product get redeem result error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getOrderRepaymentList(GetOrderRepaymentListRequest request, StreamObserver<GetOrderRepaymentListReply> responseObserver) {
        try {
            GetOrderRepaymentListReply reply = stakingProductService.getOrderRepaymentList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getProductOrderList(GetProductOrderListRequest request, StreamObserver<GetProductOrderListReply> responseObserver) {
        try {
            GetProductOrderListReply reply = stakingProductService.getProductOrderList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getProductJourList(GetProductJourListRequest request, StreamObserver<GetProductJourListReply> responseObserver) {
        try {
            GetProductJourListReply reply = stakingProductService.getProductJourList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * 为兼容原币多多接口功能，特殊新增此接口，其他业务不能使用
     */
    @Override
    public void getSimpleFinanceRecord(GetSimpleFinanceRecordRequest request, StreamObserver<GetSimpleFinanceRecordReply> responseObserver) {
        try {
            GetSimpleFinanceRecordReply reply = stakingProductService.getSimpleFinanceRecord(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void listBrokerOfPermission(ListBrokerPermissionRequest request, StreamObserver<ListBrokerPermissionReply> responseObserver) {
        try {
            ListBrokerPermissionReply reply = stakingProductService.listBorkerOfPermission(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void orgQueryProductList(OrgQueryProductListRequest request, StreamObserver<GetProductListReply> responseObserver) {
        GetProductListReply response;
        try {
            response = stakingProductService.orgQueryProductListReply(request.getHeader(), request.getType(), request.getFromId(), request.getToId(), request.getLimit());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("orgQueryProductList error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void orgGetProductOrderList(GetProductOrderListRequest request, StreamObserver<GetProductOrderListReply> responseObserver) {
        GetProductOrderListReply response;
        try {
            response = stakingProductService.orgGetProductOrderList(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("orgGetProductOrderList error", e);
            responseObserver.onError(e);
        }
    }
}
