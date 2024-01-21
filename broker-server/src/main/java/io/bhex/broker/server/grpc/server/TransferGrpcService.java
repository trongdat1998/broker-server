package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.transfer.TransferRequest;
import io.bhex.broker.grpc.transfer.TransferResponse;
import io.bhex.broker.grpc.transfer.TransferServiceGrpc;
import io.bhex.broker.server.grpc.server.service.TransferService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker.server
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2018/11/21 下午3:10
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class TransferGrpcService extends TransferServiceGrpc.TransferServiceImplBase {

    @Autowired
    private TransferService transferService;

    @Override
    public void transferToUser(TransferRequest request, StreamObserver<TransferResponse> responseObserver) {

        TransferResponse response = null;
        try {
            Boolean result
                    = transferService
                    .transferToUser(request.getUserId(), request.getTotal(), request.getClientTransferId());
            responseObserver.onNext(TransferResponse.newBuilder().setResult(result).build());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.error("transferToUser error,code={},msg={}", e.getCode(), e.getMessage());
            response = TransferResponse.newBuilder().setResult(false).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("transferToUser error", e);
            responseObserver.onError(e);
        }
    }
}
