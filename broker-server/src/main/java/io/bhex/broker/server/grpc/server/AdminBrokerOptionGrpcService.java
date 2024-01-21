package io.bhex.broker.server.grpc.server;

import org.springframework.beans.factory.annotation.Autowired;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.grpc.admin.AdminBrokerOptionServiceGrpc;
import io.bhex.broker.grpc.admin.CreateOptionReply;
import io.bhex.broker.grpc.admin.CreateOptionRequest;
import io.bhex.broker.grpc.admin.QueryOptionListRequest;
import io.bhex.broker.grpc.admin.QueryOptionListResponse;
import io.bhex.broker.server.grpc.server.service.OptionService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@GrpcLog
@GrpcService
public class AdminBrokerOptionGrpcService extends AdminBrokerOptionServiceGrpc.AdminBrokerOptionServiceImplBase {

    @Autowired
    private OptionService optionService;

    @Override
    public void createOption(CreateOptionRequest request, StreamObserver<CreateOptionReply> observer) {
        try {
            CreateOptionReply reply = optionService.createOrModifyNewOption(request);
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createOption error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryOptionList(QueryOptionListRequest request, StreamObserver<QueryOptionListResponse> observer) {
        try {
            QueryOptionListResponse reply = optionService.queryOptionList(request.getOrgId(), request.getFromId(), request.getLimit());
            observer.onNext(reply);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryOptionList error", e);
            observer.onError(e);
        }
    }
}
