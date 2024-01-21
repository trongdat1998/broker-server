package io.bhex.broker.server.grpc.server;

import io.bhex.base.account.BalanceFlowDetail;
import io.bhex.base.account.BalanceFlowsReply;
import io.bhex.base.account.BalanceServiceGrpc;
import io.bhex.base.account.GetBalanceFlowsWithPageRequest;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsBalanceService;
import io.bhex.broker.server.model.BalanceFlowSnapshot;
import io.grpc.stub.StreamObserver;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

@GrpcService
public class AdminBalanceGrpcService extends BalanceServiceGrpc.BalanceServiceImplBase {

    @Resource
    private StatisticsBalanceService statisticsBalanceService;

    @Override
    public void getBalanceFlowsWithPage(GetBalanceFlowsWithPageRequest request, StreamObserver<BalanceFlowsReply> responseObserver) {
        List<BalanceFlowSnapshot> flows = statisticsBalanceService.queryAccountBalanceFlow(request.getBaseRequest().getOrganizationId(), request.getAccountId(),
                request.getBusinessSubjectValueList(), request.getEndFlowId(), request.getLimit(), request.getTokenId());
        BalanceFlowsReply reply;
        if (CollectionUtils.isEmpty(flows)) {
            reply = BalanceFlowsReply.newBuilder().build();
        } else {
            List<BalanceFlowDetail> balanceFlowDetails = flows.stream().map(f -> {
                BalanceFlowDetail.Builder builder = BalanceFlowDetail.newBuilder();
                builder.setAccountId(f.getAccountId());
                builder.setBusinessSubjectValue(f.getBusinessSubject());
                builder.setChanged(DecimalUtil.fromBigDecimal(f.getChanged()));
                builder.setTotal(DecimalUtil.fromBigDecimal(f.getTotal()));
                builder.setBalanceFlowId(f.getId());
                builder.setCreatedTime(f.getCreatedAt().getTime());
                builder.setTokenId(f.getTokenId());
                return builder.build();
            }).collect(Collectors.toList());
            reply = BalanceFlowsReply.newBuilder().addAllBalanceFlowDetails(balanceFlowDetails).build();
        }
        responseObserver.onNext(reply);
        responseObserver.onCompleted();

    }

}
