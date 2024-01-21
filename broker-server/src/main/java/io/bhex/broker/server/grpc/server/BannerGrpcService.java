package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.banner.BannerServiceGrpc;
import io.bhex.broker.grpc.banner.QueryBannerRequest;
import io.bhex.broker.grpc.banner.QueryBannerResponse;
import io.bhex.broker.server.grpc.server.service.BannerService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 14/09/2018 5:20 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
@Slf4j
public class BannerGrpcService extends BannerServiceGrpc.BannerServiceImplBase {

    @Autowired
    private BannerService bannerService;

    @Override
    public void queryBanners(QueryBannerRequest request, StreamObserver<QueryBannerResponse> observer) {
        QueryBannerResponse response;
        try {
            response = bannerService.queryBanner(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryBannerResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryBanners error", e);
            observer.onError(e);
        }
    }
}
