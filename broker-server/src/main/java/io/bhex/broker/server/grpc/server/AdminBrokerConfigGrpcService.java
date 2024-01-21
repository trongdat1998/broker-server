package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.BrokerHomepageConfigService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 10:28 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class AdminBrokerConfigGrpcService extends AdminBrokerConfigServiceGrpc.AdminBrokerConfigServiceImplBase {

    @Autowired
    private BrokerHomepageConfigService brokerConfigService;


    @Override
    public void getBrokerWholeConfig(GetBrokerConfigRequest request, StreamObserver<AdminBrokerConfigDetail> responseObserver) {
        AdminBrokerConfigDetail brokerConfig = brokerConfigService.getBrokerWholeConfig(request.getBrokerId());

        responseObserver.onNext(brokerConfig);
        responseObserver.onCompleted();
    }

    @Override
    public void saveBrokerWholeConfig(SaveBrokerConfigRequest request, StreamObserver<SaveConfigReply> responseObserver) {
        SaveConfigReply reply = brokerConfigService.saveBrokerWholeConfig(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBrokerBasicConfig(GetBrokerConfigRequest request, StreamObserver<AdminBrokerConfigDetail> responseObserver) {
        AdminBrokerConfigDetail reply = brokerConfigService.getBrokerBasicConfig(request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getLocaleDetailConfig(GetBrokerConfigRequest request, StreamObserver<GetLocaleDetailConfigReply> responseObserver) {
        GetLocaleDetailConfigReply reply = brokerConfigService.getLocaleDetailConfigReply(request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getFeatureDetailConfig(GetBrokerConfigRequest request, StreamObserver<GetFeatureDetailConfigReply> responseObserver) {
        GetFeatureDetailConfigReply reply = brokerConfigService.getFeatureDetailConfigReply(request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void saveBrokerBasicConfig(SaveBrokerConfigRequest request, StreamObserver<SaveConfigReply> responseObserver) {
        SaveConfigReply reply = brokerConfigService.saveBrokerBasicConfig(request.getBrokerConfigDetail(), request.getSaveType(), request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void saveLocaleDetailConfig(SaveLocaleDetailConfigRequest request, StreamObserver<SaveConfigReply> responseObserver) {
        SaveConfigReply reply = brokerConfigService.saveLocaleDetailConfig(request.getLocaleDetailsList(), request.getSaveType(), request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void saveFeatureDetailConfig(SaveFeatureConfigRequest request, StreamObserver<SaveConfigReply> responseObserver) {
        SaveConfigReply reply = brokerConfigService.saveFeatureDetailConfig(request.getFeatureDetailsList(), request.getSaveType(), request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void editIndexCustomerConfig(EditIndexCustomerConfigRequest request, StreamObserver<SaveConfigReply> responseObserver) {
        SaveConfigReply reply = brokerConfigService.editIndexCustomerConfig(request.getConfigsList());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getIndexCustomerConfig(GetIndexCustomerConfigRequest request, StreamObserver<GetIndexCustomerConfigReply> responseObserver) {
        List<IndexCustomerConfig> list = brokerConfigService.getIndexCustomerConfig(request.getBrokerId(), request.getStatus(), request.getConfigTypeValue());
        responseObserver.onNext(GetIndexCustomerConfigReply.newBuilder().addAllConfigs(list).build());
        responseObserver.onCompleted();
    }

    @Override
    public void switchIndexCustomerConfig(SwitchIndexCustomerConfigRequest request, StreamObserver<SaveConfigReply> responseObserver) {
        SaveConfigReply reply = brokerConfigService.switchIndexCustomerConfig(request.getBrokerId(), request.getSwitchNewVersion());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
