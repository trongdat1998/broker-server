package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.AdminBrokerExchangeServiceGrpc;
import io.bhex.broker.grpc.admin.ChangeBrokerExchangeReply;
import io.bhex.broker.grpc.admin.ChangeBrokerExchangeRequest;
import io.bhex.broker.server.grpc.server.service.BrokerExchangeService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 22/10/2018 2:49 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class AdminBrokerExchangeGrpcService extends AdminBrokerExchangeServiceGrpc.AdminBrokerExchangeServiceImplBase {

    @Autowired
    private BrokerExchangeService brokerExchangeService;

    @Override
    public void enableContract(ChangeBrokerExchangeRequest request, StreamObserver<ChangeBrokerExchangeReply> responseObserver) {
        Boolean isOk = brokerExchangeService.enableContract(request.getBrokerId(), request.getExchangeId(), request.getExchangeName());
        ChangeBrokerExchangeReply reply = ChangeBrokerExchangeReply.newBuilder()
                .setResult(isOk)
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void disableContract(ChangeBrokerExchangeRequest request, StreamObserver<ChangeBrokerExchangeReply> responseObserver) {
        Boolean isOk = brokerExchangeService.disableContract(request.getBrokerId(), request.getExchangeId(), request.getExchangeName());
        ChangeBrokerExchangeReply reply = ChangeBrokerExchangeReply.newBuilder()
                .setResult(isOk)
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
