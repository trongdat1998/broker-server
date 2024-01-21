package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.AnnouncementService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 27/08/2018 4:31 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class AdminAnnouncementGrpcService extends AdminAnnouncementServiceGrpc.AdminAnnouncementServiceImplBase {

    @Autowired
    private AnnouncementService announcementService;

    @Override
    public void listAnnouncement(ListAnnouncementRequest request, StreamObserver<ListAnnouncementReply> responseObserver) {
        ListAnnouncementReply reply = announcementService.listAnnouncement(request.getCurrent(), request.getPageSize(), request.getBrokerId(), request.getPlatform());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getAnnouncement(GetAnnouncementRequest request, StreamObserver<AnnouncementDetail> responseObserver) {
        AnnouncementDetail reply = announcementService.getAnnouncementById(request.getAnnouncementId(), request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void createAnnouncement(CreateAnnouncementRequest request, StreamObserver<CreateAnnouncementReply> responseObserver) {
//        AnnouncementService.CreateAnnouncementParam param = AnnouncementService.CreateAnnouncementParam.builder()
//                .adminUserId(request.getAdminUserId())
//                .title(request.getTitle())
//                .content(request.getContent())
//                .status(request.getStatus())
//                .brokerId(request.getBrokerId())
//                .isPc(request.getIsPc())
//                .isApp(request.getIsApp())
//                .isNotification(request.getIsNotification())
//                .isSms(request.getIsSms())
//                .isEmail(request.getIsEmail())
//                .isAppPush(request.getIsAppPush())
//                .isDefault(request.getIsDefault())
//                .locale(request.getLocale())
//                .build();

        CreateAnnouncementReply reply = announcementService.createAnnouncement(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void updateAnnouncement(UpdateAnnouncementRequest request, StreamObserver<UpdateAnnouncementReply> responseObserver) {
        UpdateAnnouncementReply reply = announcementService.updateAnnouncement(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void pushAnnouncement(PushAnnouncementRequest request, StreamObserver<PushAnnouncementReply> responseObserver) {
        PushAnnouncementReply reply = announcementService.pushAnnouncement(request.getAnnouncementId(), request.getAdminUserId(), request.getBrokerId(), request.getStatus());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteAnnouncement(DeleteAnnouncementRequest request, StreamObserver<DeleteAnnouncementReply> responseObserver) {
        DeleteAnnouncementReply reply = announcementService.deleteAnnouncement(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
