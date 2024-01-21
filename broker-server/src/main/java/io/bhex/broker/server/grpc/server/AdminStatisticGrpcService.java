package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.KycStatisticService;
import io.bhex.broker.server.grpc.server.service.RegStatisticService;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

@GrpcService
public class AdminStatisticGrpcService extends AdminStatisticServiceGrpc.AdminStatisticServiceImplBase {
    @Autowired
    private RegStatisticService regStatisticService;
    @Autowired
    private KycStatisticService kycStatisticService;

    @Override
    public void queryAggregateRegStatistic(QueryAggregateRegStatisticRequest request, StreamObserver<QueryAggregateRegStatisticReply> responseObserver) {
        io.bhex.broker.server.model.RegStatistic regStatistic = regStatisticService.getAggregateRegStatistic(request.getBrokerId());
        RegStatistic.Builder builder = RegStatistic.newBuilder();
        if (regStatistic != null) {
            BeanCopyUtils.copyPropertiesIgnoreNull(regStatistic, builder);
        }

        QueryAggregateRegStatisticReply reply = QueryAggregateRegStatisticReply.newBuilder()
                .setRegStatistic(builder.build()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryDailyRegStatistic(QueryDailyRegStatisticRequest request, StreamObserver<QueryDailyRegStatisticReply> responseObserver) {
        List<io.bhex.broker.server.model.RegStatistic> list = regStatisticService
                .getDailyRegStatistics(request.getBrokerId(), request.getStartDate(), request.getEndDate());
        if (CollectionUtils.isEmpty(list)) {
            responseObserver.onNext(QueryDailyRegStatisticReply.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }
        List<RegStatistic> resultList = list.stream().map(statistic -> {
            RegStatistic.Builder builder = RegStatistic.newBuilder();
            if (statistic != null) {
                BeanCopyUtils.copyPropertiesIgnoreNull(statistic, builder);
            }
            return builder.build();
        }).collect(Collectors.toList());

        responseObserver.onNext(QueryDailyRegStatisticReply.newBuilder().addAllRegStatistic(resultList).build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryAggregateKycStatistic(QueryAggregateKycStatisticRequest request, StreamObserver<QueryAggregateKycStatisticReply> responseObserver) {
        io.bhex.broker.server.model.KycStatistic kycStatistic = kycStatisticService.getAggregateKycStatistic(request.getBrokerId());
        KycStatistic.Builder builder = KycStatistic.newBuilder();
        if (kycStatistic != null) {
            BeanCopyUtils.copyPropertiesIgnoreNull(kycStatistic, builder);
        }

        QueryAggregateKycStatisticReply reply = QueryAggregateKycStatisticReply.newBuilder()
                .setKycStatistic(builder.build()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryDailyKycStatistic(QueryDailyKycStatisticRequest request, StreamObserver<QueryDailyKycStatisticReply> responseObserver) {
        List<io.bhex.broker.server.model.KycStatistic> list = kycStatisticService
                .getDailyKycStatistics(request.getBrokerId(), request.getStartDate(), request.getEndDate());
        if (CollectionUtils.isEmpty(list)) {
            responseObserver.onNext(QueryDailyKycStatisticReply.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }
        List<KycStatistic> resultList = list.stream().map(statistic -> {
            KycStatistic.Builder builder = KycStatistic.newBuilder();
            if (statistic != null) {
                BeanCopyUtils.copyPropertiesIgnoreNull(statistic, builder);
            }
            return builder.build();
        }).collect(Collectors.toList());

        responseObserver.onNext(QueryDailyKycStatisticReply.newBuilder().addAllKycStatistic(resultList).build());
        responseObserver.onCompleted();
    }
}
