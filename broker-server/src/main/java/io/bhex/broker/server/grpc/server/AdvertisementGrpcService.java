package io.bhex.broker.server.grpc.server;

import com.google.protobuf.TextFormat;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.grpc.advertisement.AdvertisementServiceGrpc;
import io.bhex.broker.grpc.advertisement.GetAdvertisementOnlineListResponse;
import io.bhex.broker.server.grpc.server.service.AdvertisementService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class AdvertisementGrpcService extends AdvertisementServiceGrpc.AdvertisementServiceImplBase {

    @Autowired
    AdvertisementService advertisementService;

    public void getAdvertisementOnlineList(io.bhex.broker.grpc.advertisement.GetAdvertisementOnlineListRequest request,
                                           io.grpc.stub.StreamObserver<io.bhex.broker.grpc.advertisement.GetAdvertisementOnlineListResponse> responseObserver) {

        try {
            GetAdvertisementOnlineListResponse response = advertisementService.getAdvertisementOnlineList(request.getOrgId(), request.getLocale());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(" getAdvertisementOnlineList exception:{}", TextFormat.shortDebugString(request), e);
            responseObserver.onError(e);
        }
    }

}
