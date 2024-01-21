package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.announcement.AnnouncementServiceGrpc;
import io.bhex.broker.grpc.announcement.QueryAnnouncementRequest;
import io.bhex.broker.grpc.announcement.QueryAnnouncementResponse;
import io.bhex.broker.server.grpc.server.service.AnnouncementService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 14/09/2018 9:00 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
@Slf4j
public class AnnouncementGrpcService extends AnnouncementServiceGrpc.AnnouncementServiceImplBase {

    @Autowired
    private AnnouncementService announcementService;

    @Override
    public void queryAnnouncements(QueryAnnouncementRequest request, StreamObserver<QueryAnnouncementResponse> observer) {
        QueryAnnouncementResponse response;
        try {
            response = announcementService.queryAnnouncement(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryAnnouncementResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAnnouncements error", e);
            observer.onError(e);
        }
    }
}
