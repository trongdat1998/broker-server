package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.order.*;
import io.bhex.broker.server.grpc.server.service.ShareConfigService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 2019/6/28 2:53 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService
public class ShareConfigGrpcService extends ShareConfigServiceGrpc.ShareConfigServiceImplBase {

    @Autowired
    private ShareConfigService shareConfigService;

    @Override
    public void shareConfigInfoByAdmin(ShareConfigInfoByAdminRequest request, StreamObserver<ShareConfigInfoByAdminReply> responseObserver) {
        ShareConfigInfoByAdminReply reply = shareConfigService.shareConfigInfoByAdmin(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryShareConfigInfo(QueryShareConfigInfoRequest request, StreamObserver<QueryShareConfigInfoReply> responseObserver) {
        QueryShareConfigInfoReply reply = shareConfigService.shareConfigInfo(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void saveShareConfigInfo(SaveShareConfigInfoRequest request, StreamObserver<SaveShareConfigInfoReply> responseObserver) {
        SaveShareConfigInfoReply reply = shareConfigService.saveShareConfigInfo(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
