package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductAdminService;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductOrderService;
import io.bhex.broker.server.model.staking.StakingProductRebate;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

@Slf4j
@GrpcService
public class AdminStakingProductGrpcService extends AdminStakingProductServiceGrpc.AdminStakingProductServiceImplBase {

    @Autowired
    StakingProductAdminService stakingProductAdminService;

    @Resource
    private StakingProductOrderService stakingProductOrderService;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Override
    public void saveProduct(AdminSaveProductRequest request, StreamObserver<AdminSaveProductReply> responseObserver) {
        try {
            AdminSaveProductReply reply = stakingProductAdminService.saveProduct(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getProductDetail(AdminGetProductDetailRequest request, StreamObserver<AdminGetProductDetailReply> responseObserver) {
        try {
            AdminGetProductDetailReply reply = stakingProductAdminService.getProductDetail(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getProductList(AdminGetProductListRequest request, StreamObserver<AdminGetProductListReply> responseObserver) {
        try {
            AdminGetProductListReply reply = stakingProductAdminService.getProductList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void onlineProduct(AdminOnlineProductRequest request, StreamObserver<AdminCommonResponse> responseObserver) {
        try {
            AdminCommonResponse reply = stakingProductAdminService.onlineProduct(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setBrokerFixedProductPermission(AdminSetBrokerFixedProductPermissionRequest request, StreamObserver<AdminCommonResponse> responseObserver) {

        try {
            AdminCommonResponse reply = stakingProductAdminService.setBrokerFixedProductPermission(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setBrokerFixedLockProductPermission(AdminSetBrokerFixedLockProductPermissionRequest request, StreamObserver<AdminCommonResponse> responseObserver) {
        try {
            AdminCommonResponse reply = stakingProductAdminService.setBrokerFixedLockProductPermission(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getBrokerProductPermission(AdminGetBrokerProductPermissionRequest request, StreamObserver<AdminGetBrokerProductPermissionReply> responseObserver) {

        try {
            AdminGetBrokerProductPermissionReply reply = stakingProductAdminService.getBrokerProductPermission(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryBrokerProductUndoRebate(AdminQueryBrokerProductUndoRebateRequest request, StreamObserver<AdminQueryBrokerProductUndoRebateReply> responseObserver) {
        try {
            AdminQueryBrokerProductUndoRebateReply reply = stakingProductAdminService.queryBrokerProductUndoRebate(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryBrokerProductHistoryRebate(AdminQueryBrokerProductHistoryRebateRequest request, StreamObserver<AdminQueryBrokerProductHistoryRebateReply> responseObserver) {
        try {
            AdminQueryBrokerProductHistoryRebateReply reply = stakingProductAdminService.queryBrokerProductHistoryRebate(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * 处理转账请求
     *
     * @param request
     * @param observer
     */
    @Override
    public void dividendTransfer(StakingProductDividendTransferRequest request, StreamObserver<StakingProductDividendTransferResponse> observer) {
        StakingProductDividendTransferResponse response;
        try {
            Integer rtnCode = stakingProductOrderService.dividendTransfer(request.getOrgId()
                    , request.getProductId()
                    , request.getProductRebateId());
            response = StakingProductDividendTransferResponse.newBuilder().setRet(rtnCode).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = StakingProductDividendTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("staking dividend transfer error", e);
            observer.onError(e);
        }
    }

    /**
     * 取消派息
     *
     * @param request
     * @param observer
     */
    @Override
    public void cancelDividend(StakingProductCancelDividendRequest request, StreamObserver<StakingProductCancelDividendResponse> observer) {
        StakingProductCancelDividendResponse response;
        try {
            Boolean rtn = stakingProductOrderService.cancelDividend(request.getOrgId()
                    , request.getProductId()
                    , request.getProductRebateId());
            Integer rtnCode = rtn ? BrokerErrorCode.SUCCESS.code() : BrokerErrorCode.FINANCE_REDEEM_ERROR.code();
            response = StakingProductCancelDividendResponse.newBuilder().setRet(rtnCode).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = StakingProductCancelDividendResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("staking cancel dividend error", e);
            observer.onError(e);
        }
    }

    /**
     * 重算派息
     *
     * @param request
     * @param observer
     */
    @Override
    public void calcInterest(StakingCalcInterestRequest request, StreamObserver<StakingCalcInterestReply> observer) {
        StakingCalcInterestReply reply;
        try {

            //先将原派息记录置为无效，再新建派息记录
            StakingProductRebate productRebate = stakingProductAdminService.reCalcInterestUpdateOldRebate(request.getOrgId(), request.getProductId(), request.getProductRebateId(), request.getRebateAmount(), request.getRebateRate(), request.getTokenId());

            CompletableFuture.runAsync(() ->
                            stakingProductOrderService.calcInterest(productRebate.getOrgId()
                                    , productRebate.getProductId()
                                    , productRebate.getId())
                    , taskExecutor);

            observer.onNext(StakingCalcInterestReply.newBuilder().build());
            observer.onCompleted();
        } catch (BrokerException e) {
            reply = StakingCalcInterestReply.newBuilder().setCode(e.getCode()).build();
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("staking calc intrest error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryBrokerProductOrder(QueryBrokerProductOrderRequest request, StreamObserver<QueryBrokerProductOrderReply> observer) {

        QueryBrokerProductOrderReply reply;
        try {
            reply = stakingProductAdminService.queryBrokerProductOrder(request);
            observer.onNext(reply);
            observer.onCompleted();
        } catch (BrokerException e) {
            reply = QueryBrokerProductOrderReply.newBuilder().build();
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryBrokerProductOrder error", e);
            observer.onError(e);
        }
    }


    /**
     * 查询活期资产持仓列表
     */
    @Override
    public void queryCurrentProductAsset(AdminQueryCurrentProductAssetRequest request, StreamObserver<AdminQueryCurrentProductAssetReply> observer) {
        AdminQueryCurrentProductAssetReply reply;
        try {
            reply = stakingProductAdminService.queryCurrentProductAsset(request);
            observer.onNext(reply);
            observer.onCompleted();
        } catch (BrokerException e) {
            reply = AdminQueryCurrentProductAssetReply.newBuilder().build();
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryCurrentProductAsset error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getProductRepaymentSchedule(GetProductRepaymentScheduleRequest request, StreamObserver<GetProductRepaymentScheduleReply> observer) {

        GetProductRepaymentScheduleReply reply;
        try {
            reply = stakingProductAdminService.getProductRepaymentSchedule(request);
            observer.onNext(reply);
            observer.onCompleted();
        } catch (BrokerException e) {
            reply = GetProductRepaymentScheduleReply.newBuilder().build();
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getProductRepaymentSchedule error", e);
            observer.onError(e);
        }
    }

    /**
     * 获取活期产品派息记录
     */
    public void getCurrentProductRepaymentSchedule(GetCurrentProductRepaymentScheduleRequest request, StreamObserver<GetCurrentProductRepaymentScheduleReply> observer) {

        GetCurrentProductRepaymentScheduleReply reply;
        try {
            reply = stakingProductAdminService.getCurrentProductRepaymentSchedule(request);
            observer.onNext(reply);
            observer.onCompleted();
        } catch (BrokerException e) {
            reply = GetCurrentProductRepaymentScheduleReply.newBuilder().build();
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getCurrentProductRepaymentSchedule error", e);
            observer.onError(e);
        }
    }

    /**
     * 获取活期产品派息计划
     */
    public void getCurrentProductRebateList(GetCurrentProductRebateListRequest request, StreamObserver<GetCurrentProductRebateListReply> observer) {
        GetCurrentProductRebateListReply reply;
        try {
            reply = stakingProductAdminService.getCurrentProductRebateList(request);
            observer.onNext(reply);
            observer.onCompleted();
        } catch (BrokerException e) {
            reply = GetCurrentProductRebateListReply.newBuilder().build();
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getCurrentProductRebateList error", e);
            observer.onError(e);
        }
    }

    /**
     * 设置券商理财活期产品权限
     */
    @Override
    public void setBrokerCurrentProductPermission(AdminSetBrokerCurrentProductPermissionRequest request, StreamObserver<AdminCommonResponse> observer) {
        AdminCommonResponse reply;
        try {
            reply = stakingProductAdminService.setBrokerCurrentProductPermission(request);
            observer.onNext(reply);
            observer.onCompleted();
        } catch (BrokerException e) {
            reply = AdminCommonResponse.newBuilder().build();
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setBrokerCurrentProductPermission error", e);
            observer.onError(e);
        }
    }

}
