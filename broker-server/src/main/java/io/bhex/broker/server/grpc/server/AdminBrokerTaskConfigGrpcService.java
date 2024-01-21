package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.AdminBrokerTaskConfigService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService
public class AdminBrokerTaskConfigGrpcService extends AdminBrokerTaskConfigServiceGrpc.AdminBrokerTaskConfigServiceImplBase {

    @Resource
    private AdminBrokerTaskConfigService adminBrokerTaskConfigService;

    @Override
    public void getBrokerTaskConfigs(GetBrokerTaskConfigsRequest request, StreamObserver<GetBrokerTaskConfigsReply> observer) {
        GetBrokerTaskConfigsReply response;
        try {
            response = adminBrokerTaskConfigService.getBrokerTaskConfigs(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerTaskConfigsReply.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getBrokerTaskConfigs", e);
            observer.onError(e);
        }
    }

    @Override
    public void saveBrokerTaskConfig(SaveBrokerTaskConfigRequest request, StreamObserver<SaveBrokerTaskConfigReply> observer) {
        SaveBrokerTaskConfigReply response;
        try {
            response = adminBrokerTaskConfigService.saveBrokerTaskConfig(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SaveBrokerTaskConfigReply.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("saveBrokerTaskConfig", e);
            observer.onError(e);
        }
    }
}
