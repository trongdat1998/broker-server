/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/9/9
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.notice.*;
import io.bhex.broker.server.grpc.server.service.NoticeTemplateService;
import io.bhex.broker.server.model.NoticeTemplate;
import io.grpc.stub.StreamObserver;

import javax.annotation.Resource;

@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class NoticeTemplateGrpcService extends NoticeTemplateServiceGrpc.NoticeTemplateServiceImplBase {

    @Resource
    private NoticeTemplateService noticeTemplateService;

    @Override
    public void queryVerifyCodeTemplate(QueryVerifyCodeTemplateRequest request, StreamObserver<QueryVerifyCodeTemplateResponse> observer) {
        QueryVerifyCodeTemplateResponse response = QueryVerifyCodeTemplateResponse.getDefaultInstance();
        observer.onNext(response);
        observer.onCompleted();
//        try {
//            response = noticeTemplateService.queryVerifyCodeTemplate();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (BrokerException e) {
//            response = QueryVerifyCodeTemplateResponse.newBuilder().setRet(e.getCode()).build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (Exception e) {
//            observer.onError(e);
//        }
    }

    @Override
    public void queryNoticeTemplate(QueryNoticeTemplateRequest request, StreamObserver<QueryNoticeTemplateResponse> observer) {
        QueryNoticeTemplateResponse response = QueryNoticeTemplateResponse.getDefaultInstance();
        observer.onNext(response);
        observer.onCompleted();
//        try {
//            NoticeTemplate template = noticeTemplateService.queryDefaultNoticeTemplate(request.getOrgId(), request.getNoticeType(),
//                    request.getBusinessType().name(), request.getLanguage());
//            if (template != null) {
//                response = QueryNoticeTemplateResponse.newBuilder().setRet(0)
//                        .setTemplateId(template.getTemplateId())
//                        .setTemplate(template.toGrpcData())
//                        .build();
//            } else {
//                response = QueryNoticeTemplateResponse.getDefaultInstance();
//            }
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (BrokerException e) {
//            response = QueryNoticeTemplateResponse.newBuilder().setRet(e.getCode()).build();
//            observer.onNext(response);
//            observer.onCompleted();
//        } catch (Exception e) {
//            observer.onError(e);
//        }
    }
}
