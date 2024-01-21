package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.customlabel.*;
import io.bhex.broker.server.grpc.server.service.CustomLabelService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 2019/12/13 9:41 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService
public class CustomLabelGrpcService extends CustomLabelServiceGrpc.CustomLabelServiceImplBase {

    @Autowired
    private CustomLabelService customLabelService;

    @Override
    public void getBrokerCustomLabel(GetBrokerCustomLabelRequest request, StreamObserver<GetBrokerCustomLabelReply> observer) {
        GetBrokerCustomLabelReply response;
        try {
            response = customLabelService.getBrokerCustomLabel(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerCustomLabelReply.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void saveUserCustomLabel(SaveUserCustomLabelRequest request, StreamObserver<SaveUserCustomLabelReply> observer) {
        SaveUserCustomLabelReply response;
        try {
            response = customLabelService.saveUserCustomLabel(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SaveUserCustomLabelReply.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void delUserCustomLabel(DelUserCustomLabelRequest request, StreamObserver<DelUserCustomLabelReply> observer) {
        DelUserCustomLabelReply response;
        try {
            response = customLabelService.delUserCustomLabel(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DelUserCustomLabelReply.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }
}
