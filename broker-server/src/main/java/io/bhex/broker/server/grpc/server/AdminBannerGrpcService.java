package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.BannerService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 28/08/2018 7:51 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class AdminBannerGrpcService extends AdminBannerServiceGrpc.AdminBannerServiceImplBase {

    @Autowired
    private BannerService bannerService;

    @Override
    public void listBanner(ListBannerRequest request, StreamObserver<ListBannerReply> responseObserver) {
        ListBannerReply reply = bannerService.listBanner(request.getCurrent(), request.getPageSize(),
                request.getBrokerId(), request.getPlatform(), request.getBannerPosition());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBannerById(GetBannerByIdRequest request, StreamObserver<BannerDetail> responseObserver) {
        BannerDetail reply = bannerService.getBannerById(request.getBannerId(), request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void createBanner(CreateBannerRequest request, StreamObserver<CreateBannerReply> responseObserver) {
//        BannerService.SaveBannerParam param = BannerService.SaveBannerParam.builder()
//                .adminUserId(request.getAdminUserId())
//                .brokerId(request.getBrokerId())
//                .type(request.getType())
//                .pageUrl(request.getPageUrl())
//                .rank(request.getRank())
//                .beginAt(request.getBeginAt())
//                .endAt(request.getEndAt())
//                .remark(request.getRemark())
//                .isWeb(request.getIsWeb())
//                .isAndroid(request.getIsAndroid())
//                .isIos(request.getIsIos())
//                .isDefault(request.getIsDefault())
//                .imageUrl(request.getImageUrl())
//                .locale(request.getLocale())
//                .title(request.getTitle())
//                .content(request.getContent())
//                .build();

        CreateBannerReply reply = bannerService.createBanner(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void updateBanner(UpdateBannerRequest request, StreamObserver<UpdateBannerReply> responseObserver) {
//        BannerService.SaveBannerParam param = BannerService.SaveBannerParam.builder()
//                .bannerId(request.getBannerId())
//                .localeDetailId(request.getLocaleDetailId())
//                .adminUserId(request.getAdminUserId())
//                .brokerId(request.getBrokerId())
//                .type(request.getType())
//                .pageUrl(request.getPageUrl())
//                .rank(request.getRank())
//                .beginAt(request.getBeginAt())
//                .endAt(request.getEndAt())
//                .remark(request.getRemark())
//                .isWeb(request.getIsWeb())
//                .isAndroid(request.getIsAndroid())
//                .isIos(request.getIsIos())
//                .isDefault(request.getIsDefault())
//                .imageUrl(request.getImageUrl())
//                .locale(request.getLocale())
//                .title(request.getTitle())
//                .content(request.getContent())
//                .build();
        UpdateBannerReply reply = bannerService.updateBanner(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteBanner(DeleteBannerRequest request, StreamObserver<DeleteBannerReply> responseObserver) {
        DeleteBannerReply reply = bannerService.deleteBanner(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
