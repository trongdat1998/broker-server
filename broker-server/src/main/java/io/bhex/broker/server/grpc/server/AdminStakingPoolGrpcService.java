package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.grpc.server.service.staking.StakingStakingAdminService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GrpcService
public class AdminStakingPoolGrpcService extends AdminStakingPoolServiceGrpc.AdminStakingPoolServiceImplBase {

    @Autowired
    StakingStakingAdminService stakingStakingAdminService;

    @Override
    public void saveStakingPool(AdminSaveStakingPoolRequest request, StreamObserver<AdminSaveStakingPoolReply> responseObserver) {
        try {
            AdminSaveStakingPoolReply reply = stakingStakingAdminService.saveStakingPool(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getStakingPoolDetail(AdminGetStakingPoolDetailRequest request, StreamObserver<AdminGetStakingPoolDetailReply> responseObserver) {
        try {
            AdminGetStakingPoolDetailReply reply = stakingStakingAdminService.getStakingPoolDetail(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getStakingPoolList(AdminGetStakingPoolListRequest request, StreamObserver<AdminGetStakingPoolListReply> responseObserver) {
        try {
            AdminGetStakingPoolListReply reply = stakingStakingAdminService.getStakingPoolList(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void onlineStakingPool(AdminOnlineStakingPoolRequest request, StreamObserver<AdminCommonResponse> responseObserver) {
        try {
            AdminCommonResponse reply = stakingStakingAdminService.onlineStakingPool(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

}
