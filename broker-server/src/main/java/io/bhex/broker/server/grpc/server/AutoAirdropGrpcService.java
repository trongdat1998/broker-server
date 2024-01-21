package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.airdrop.*;
import io.bhex.broker.server.grpc.server.service.AutoAirdropService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 30/11/2018 3:14 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class AutoAirdropGrpcService extends AutoAirdropServiceGrpc.AutoAirdropServiceImplBase {

    @Autowired
    private AutoAirdropService autoAirdropService;

    @Override
    public void saveAutoAirdrop(SaveAutoAirdropInfoRequest request, StreamObserver<SaveAutoAirdropInfoResponse> responseObserver) {
        SaveAutoAirdropInfoResponse response = autoAirdropService.saveAutoAirdrop(request);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getAutoAirdrop(GetAutoAirdropInfoRequest request, StreamObserver<AutoAirdropInfo> responseObserver) {
        AutoAirdropInfo response = autoAirdropService.getAutoAirdrop(request);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
