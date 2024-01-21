package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.draw.*;
import io.bhex.broker.server.grpc.server.service.activity.ActivityDrawService;
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
@Slf4j
@GrpcService
public class ActivityDrawGrpcService extends ActivityDrawServiceGrpc.ActivityDrawServiceImplBase {

    @Autowired
    private ActivityDrawService activityDrawService;


    public void getActivityDrawItemList(GetActivityDrawItemListRequest request,
                                        StreamObserver<GetActivityDrawItemListResponse> observer) {

        GetActivityDrawItemListResponse response;
        try {
            response = activityDrawService.getActivityDrawItemListResponse(request.getHeader().getOrgId());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" getActivityDrawItemList brokerException:", e);
            response = GetActivityDrawItemListResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" getActivityDrawItemList exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();


    }

    public void getUserActivityDrawChance(GetUserActivityDrawChanceRequest request,
                                          StreamObserver<GetUserActivityDrawChanceResponse> observer) {
        GetUserActivityDrawChanceResponse response;
        try {
            response = activityDrawService.getUserActivityDrawChance(request.getHeader());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" getUserActivityDrawChance brokerException:", e);
            response = GetUserActivityDrawChanceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" getUserActivityDrawChance exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    public void getShareUserActivityDrawChance(GetShareUserActivityDrawChanceRequest request,
                                               StreamObserver<GetShareUserActivityDrawChanceResponse> observer) {
        GetShareUserActivityDrawChanceResponse response;
        try {
            response = activityDrawService.getShareUserActivityDrawChance(request.getHeader(), request.getTicket(), request.getOpenId());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" getShareUserActivityDrawChance brokerException:", e);
            response = GetShareUserActivityDrawChanceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" getShareUserActivityDrawChance exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();
    }


    public void userLuckDraw(UserLuckDrawRequest request,
                             StreamObserver<UserLuckDrawResponse> observer) {
        UserLuckDrawResponse response;
        try {
            response = activityDrawService.userLuckDraw(request.getHeader());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" userLuckDraw brokerException:", e);
            response = UserLuckDrawResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" userLuckDraw exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    public void shareUserLuckDraw(ShareUserLuckDrawRequest request,
                                  StreamObserver<ShareUserLuckDrawResponse> observer) {

        ShareUserLuckDrawResponse response;
        try {
            response = activityDrawService.shareUserLuckDraw(request.getHeader(), request.getTicket(),
                    request.getOpenId(), request.getUserName(), request.getHeaderUrl());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" shareUserLuckDraw brokerException:", e);
            response = ShareUserLuckDrawResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" shareUserLuckDraw exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    public void getUserDrawRecordList(GetUserDrawRecordListRequest request,
                                      StreamObserver<GetDrawRecordListResponse> observer) {
        GetDrawRecordListResponse response;
        try {
            response = activityDrawService.getUserDrawRecordList(request.getHeader(), request.getType(),
                    request.getFromId(), request.getEndId(), request.getLimit());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" getUserDrawRecordList brokerException:", e);
            response = GetDrawRecordListResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" getUserDrawRecordList exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    public void getShareDrawRecordList(GetShareDrawRecordListRequest request,
                                       StreamObserver<GetDrawRecordListResponse> observer) {
        GetDrawRecordListResponse response;
        try {
            response = activityDrawService.getShareDrawRecordList(request.getHeader(), request.getTicket(),
                    request.getType(), request.getFromId(), request.getEndId(), request.getLimit());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" getShareDrawRecordList brokerException:", e);
            response = GetDrawRecordListResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" getShareDrawRecordList exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();
    }

    public void getLastDrawRecordList(GetLastDrawRecordListRequest request,
                                      StreamObserver<GetDrawRecordListResponse> observer) {
        GetDrawRecordListResponse response;
        try {
            response = activityDrawService.getLastDrawRecordList(request.getHeader());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" getLastDrawRecordList brokerException:", e);
            response = GetDrawRecordListResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" getLastDrawRecordList exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();

    }

    public void getActivityDrawUserName(GetActivityDrawUserNameRequest request,
                                        StreamObserver<GetActivityDrawUserNameResponse> observer) {

        GetActivityDrawUserNameResponse response;
        try {
            response = activityDrawService.getActivityDrawUserName(request.getTicket());
            observer.onNext(response);
        } catch (BrokerException e) {
            log.info(" getLastDrawRecordList brokerException:", e);
            response = GetActivityDrawUserNameResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
        } catch (Exception e) {
            log.error(" getLastDrawRecordList exception:", e);
            observer.onError(e);
        }
        observer.onCompleted();


    }
}
