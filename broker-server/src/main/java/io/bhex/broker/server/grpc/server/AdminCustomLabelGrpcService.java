package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.AdminCustomLabelService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 2019/12/13 2:34 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService
public class AdminCustomLabelGrpcService extends AdminCustomLabelServiceGrpc.AdminCustomLabelServiceImplBase {

    @Resource
    private AdminCustomLabelService adminCustomLabelService;

    @Override
    public void queryCustomLabel(QueryCustomLabelRequest request, StreamObserver<QueryCustomLabelReply> observer) {
        QueryCustomLabelReply response;
        try {
            response = adminCustomLabelService.queryCustomLabel(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryCustomLabelReply.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryCustomLabel", e);
            observer.onError(e);
        }
    }

    @Override
    public void saveCustomLabel(SaveCustomLabelRequest request, StreamObserver<SaveCustomLabelReply> observer) {
        SaveCustomLabelReply response;
        try {
            response = adminCustomLabelService.saveCustomLabel(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SaveCustomLabelReply.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("saveCustomLabel", e);
            observer.onError(e);
        }
    }

    @Override
    public void delCustomLabel(DelCustomLabelRequest request, StreamObserver<DelCustomLabelReply> observer) {
        DelCustomLabelReply response;
        try {
            response = adminCustomLabelService.delCustomLabel(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DelCustomLabelReply.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("delCustomLabel", e);
            observer.onError(e);
        }
    }
}
