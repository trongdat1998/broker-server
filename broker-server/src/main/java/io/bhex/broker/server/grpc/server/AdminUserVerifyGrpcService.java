package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.GetPersonalVerifyInfoResponse;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.grpc.server.service.UserVerifyService;
import io.bhex.broker.server.model.UserVerifyHistory;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 23/08/2018 9:15 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class AdminUserVerifyGrpcService extends AdminUserVerifyServiceGrpc.AdminUserVerifyServiceImplBase {

    @Autowired
    private UserVerifyService userVerifyService;

    @Resource
    private UserService userService;

    @Override
    public void queryUnverifiedUser(QueryUnverifiedUserRequest request, StreamObserver<ListUnverifiedUserReply> responseObserver) {
        ListUnverifiedUserReply reply = userVerifyService.queryUnverifiedUser(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryUserVerifyList(QueryUserVerifyListRequest request, StreamObserver<QueryUserVerifyListReply> responseObserver) {
        QueryUserVerifyListReply reply = userVerifyService.queryUserVerifyList(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getUnverifiedUser(GetVerifyUserRequest request, StreamObserver<UserVerifyDetail> responseObserver) {
        UserVerifyDetail reply = userVerifyService.getVerifyUser(request.getUserVerifyId(), request.getBrokerId(), request.getLocale(), request.getDecryptUrl());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void updateVerifyUser(UpdateVerifyUserRequest request, StreamObserver<UpdateVerifyUserReply> responseObserver) {
        UpdateVerifyUserReply reply = userVerifyService.updateVerifyUser(request.getUserVerifyId(), request.getVerifyStatus(), request.getReasonId(), request.getAdminUserId(), request.getBrokerId(), request.getRemark());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listVerifyReason(ListVerifyReasonRequest request, StreamObserver<ListVerifyReasonReply> responseObserver) {
        ListVerifyReasonReply reply = userVerifyService.listVerifyReason(request.getLocale());
        if (CollectionUtils.isEmpty(reply.getVerifyReasonDetailsList())) {
            reply = userVerifyService.listVerifyReason("en_US");
        }
        if (CollectionUtils.isEmpty(reply.getVerifyReasonDetailsList())) {
            reply = userVerifyService.listVerifyReason("zh_CN");
        }
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listVerifyHistory(ListVerifyHistoryRequest request, StreamObserver<ListVerifyHistoryReply> responseObserver) {
        ListVerifyHistoryReply reply = userVerifyService.listVerifyHistory(request.getUserVerifyId(), request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getPersonalKyc(GetPersonalKycRequest request, StreamObserver<GetPersonalKycResponse> observer) {
        GetPersonalKycResponse response;
        try {
            Header header = Header.newBuilder()
                    .setUserId(request.getUserId())
                    .setLanguage(request.getLanguage())
                    .build();
            GetPersonalVerifyInfoResponse kyc = userService.getUserVerifyInfo(header);

            GetPersonalKycResponse.Builder builder = GetPersonalKycResponse.newBuilder();
            if (kyc.getVerifyInfo() != null && !kyc.getVerifyInfo().getNationality().equals("")) {
                BeanUtils.copyProperties(kyc.getVerifyInfo(), builder);
                builder.setRet(0);
                if (builder.getVerifyStatus() == UserVerifyStatus.PASSED.value()) {
                    UserVerifyHistory history = userVerifyService.getPassedVerify(kyc.getVerifyInfo().getUserVerifyId());
                    if (history != null) {
                        builder.setPassedTime(history.getCreatedAt().getTime());
                    }
                }
            } else {
                builder.setRet(1);
            }
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetPersonalKycResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void listUpdateUserByDate(ListUpdateUserByDateRequest request, StreamObserver<ListUpdateUserByDateReply> responseObserver) {
        ListUpdateUserByDateReply reply = userService.listUpdateUserByDate(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }


    @Override
    public void addBrokerKycConfig(AddBrokerKycConfigRequest request, StreamObserver<AddBrokerKycConfigReply> responseObserver) {
        userVerifyService.addBrokerKycConfig(request);
        responseObserver.onNext(AddBrokerKycConfigReply.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void degradeBrokerKycLevel(DegradeBrokerKycLevelRequest request, StreamObserver<DegradeBrokerKycLevelReply> responseObserver) {
        userVerifyService.degradeBrokerKycLevel(request);
        responseObserver.onNext(DegradeBrokerKycLevelReply.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void openThirdKycAuth(OpenThirdKycAuthRequest request, StreamObserver<OpenThirdKycAuthReply> responseObserver) {
        userVerifyService.openThirdKycAuth(request);
        responseObserver.onNext(OpenThirdKycAuthReply.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getBrokerKycConfigs(GetBrokerKycConfigsRequest request, StreamObserver<GetBrokerKycConfigsReply> responseObserver) {
        responseObserver.onNext(GetBrokerKycConfigsReply.newBuilder(userVerifyService.getBrokerKycConfigs(request.getBrokerId())).build());
        responseObserver.onCompleted();
    }
}
