package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.useraction.QueryLogsReply;
import io.bhex.broker.grpc.useraction.QueryLogsRequest;
import io.bhex.broker.grpc.useraction.UserActionLogServiceGrpc;
import io.bhex.broker.server.grpc.server.service.UserActionLogService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService
public class UserActionLogGrpcService extends UserActionLogServiceGrpc.UserActionLogServiceImplBase {

    @Resource
    private UserActionLogService userActionLogService;

    @Override
    public void queryLogs(QueryLogsRequest request, StreamObserver<QueryLogsReply> responseObserver) {
        QueryLogsReply response = null;
        try {
            responseObserver.onNext(userActionLogService.queryLogs(request));
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.error("queryLogs error,code={},msg={}", e.getCode(), e.getMessage());
            response = QueryLogsReply.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryLogs error", e);
            responseObserver.onError(e);
        }
    }
}
