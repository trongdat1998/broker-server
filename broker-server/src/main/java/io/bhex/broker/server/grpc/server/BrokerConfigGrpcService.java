package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.basic.QueryTokenResponse;
import io.bhex.broker.grpc.broker.*;
import io.bhex.broker.server.grpc.server.service.BrokerHomepageConfigService;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 5:20 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class BrokerConfigGrpcService extends BrokerConfigServiceGrpc.BrokerConfigServiceImplBase {

    @Autowired
    private BrokerHomepageConfigService brokerConfigService;

    @Override
    public void queryBrokerConfigs(QueryBrokerConfigRequest request, StreamObserver<QueryBrokerConfigResponse> responseObserver) {
        ServerCallStreamObserver<QueryBrokerConfigResponse> observer = (ServerCallStreamObserver<QueryBrokerConfigResponse>) responseObserver;
        observer.setCompression("gzip");
        QueryBrokerConfigResponse response = brokerConfigService.listHomepageConfig(request.getHeader());
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void previewBrokerConfig(PreviewBrokerConfigRequest request, StreamObserver<BrokerConfigDetail> responseObserver) {
        BrokerConfigDetail brokerConfigDetail = brokerConfigService.previewConfig(request.getHeader().getOrgId(), request.getHeader().getLanguage());

        responseObserver.onNext(brokerConfigDetail);
        responseObserver.onCompleted();
    }

    @Override
    public void queryAppIndexConfigs(QueryAppIndexConfigsRequest request, StreamObserver<QueryAppIndexConfigsResponse> responseObserver) {
        ServerCallStreamObserver<QueryAppIndexConfigsResponse> observer = (ServerCallStreamObserver<QueryAppIndexConfigsResponse>) responseObserver;
        observer.setCompression("gzip");
        QueryAppIndexConfigsResponse response = brokerConfigService.queryAppIndexConfigs(request);
        observer.onNext(response);
        observer.onCompleted();
    }
}
