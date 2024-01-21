package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.activity.experiencefund.*;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.server.grpc.server.service.ActivityExperienceFundService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Date: 2020/3/3 下午2:31
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Slf4j
@GrpcService
public class ActivityExperienceFundGrpcService extends ExperienceFundServiceGrpc.ExperienceFundServiceImplBase {

    @Resource
    private ActivityExperienceFundService experienceFundService;

    @Override
    public void saveExperienceFundInfo(SaveExperienceFundInfoRequest request, StreamObserver<BasicRet> responseObserver) {
        experienceFundService.saveExperienceFundInfo(request);

        responseObserver.onNext(BasicRet.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryExperienceFunds(QueryExperienceFundsRequest request, StreamObserver<QueryExperienceFundsResponse> responseObserver) {
        List<ExperienceFundInfo> list = experienceFundService.listExperienceFundInfos(request);

        responseObserver.onNext(QueryExperienceFundsResponse.newBuilder().addAllExperienceFundInfo(list).build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryTransferRecords(QueryTransferRecordsRequest request, StreamObserver<QueryTransferRecordsResponse> responseObserver) {
        List<ExperienceFundTransferRecord> list = experienceFundService.queryTransferList(request);

        responseObserver.onNext(QueryTransferRecordsResponse.newBuilder().addAllRecords(list).build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryExperienceFundDetail(QueryExperienceFundDetailRequest request, StreamObserver<ExperienceFundInfo> responseObserver) {
        ExperienceFundInfo detail = experienceFundService.detail(request.getBrokerId(), request.getId());
        responseObserver.onNext(detail);
        responseObserver.onCompleted();
    }

    @Override
    public void checkAccountJoinedActivity(CheckAccountJoinedActivityRequest request, StreamObserver<CheckAccountJoinedActivityResponse> responseObserver) {
        Map<Long, Boolean> result = experienceFundService.checkJoinedActivity(request);
        responseObserver.onNext(CheckAccountJoinedActivityResponse.newBuilder().putAllItem(result).build());
        responseObserver.onCompleted();
    }
}
