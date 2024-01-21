package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.invite.*;
import io.bhex.broker.server.grpc.server.service.InviteService;
import io.bhex.broker.server.grpc.server.service.InviteTaskService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class InviteGrpcService extends InviteServiceGrpc.InviteServiceImplBase {

    @Autowired
    InviteService inviteService;

    @Autowired
    InviteTaskService inviteTaskService;

    @Override
    public void getInviteInfo(GetInviteInfoRequest request, StreamObserver<GetInviteInfoResponse> observer) {

        GetInviteInfoResponse response;
        try {
            response = inviteService.getInviteInfo(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetInviteInfoResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getInviteInfo exception:", e);
            observer.onError(e);
        }

    }


    @Override
    public void getInviteBonusRecord(GetInviteBonusRecordRequest request, StreamObserver<GetInviteBonusRecordResponse> observer) {

        GetInviteBonusRecordResponse response;
        try {
            response = inviteService.getInviteBonusRecord(request.getHeader(), request.getNextId(), request.getBeforeId(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetInviteBonusRecordResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getInviteBonusRecord exception:", e);
            observer.onError(e);
        }

    }


    @Override
    public void getInviteFeeBackActivity(GetInviteFeeBackActivityRequest request,
                                         StreamObserver<GetInviteFeeBackActivityResponse> observer) {
        GetInviteFeeBackActivityResponse response;
        try {
            response = inviteService.getInviteFeeBackActivity(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetInviteFeeBackActivityResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getInviteFeeBackActivity exception:", e);
            observer.onError(e);
        }
    }


    @Override
    public void updateInviteFeeBackLevel(UpdateInviteFeeBackLevelRequest request,
                                         StreamObserver<UpdateInviteFeeBackLevelResponse> observer) {
        UpdateInviteFeeBackLevelResponse response;
        try {
            Integer levelCondition = StringUtils.isEmpty(request.getLevelCondition()) ? 0 : Integer.valueOf(request.getLevelCondition());
            BigDecimal directRate = StringUtils.isEmpty(request.getDirectRate()) ? BigDecimal.ZERO : new BigDecimal(request.getDirectRate());
            BigDecimal indirectRate = StringUtils.isEmpty(request.getIndirectRate()) ? BigDecimal.ZERO : new BigDecimal(request.getIndirectRate());
            response = inviteService.updateInviteFeeBackLevel(request.getHeader(), request.getActId(), request.getLevelId(),
                    levelCondition, directRate, indirectRate);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateInviteFeeBackLevelResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" updateInviteFeeBackLevel exception:", e);
            observer.onError(e);
        }
    }

    @Override
    public void updateInviteFeeBackActivity(UpdateInviteFeeBackActivityRequest request,
                                            StreamObserver<UpdateInviteFeeBackActivityResponse> observer) {
        UpdateInviteFeeBackActivityResponse response;
        try {
            response = inviteService.updateInviteFeeBackActivity(request.getHeader(), request.getActId(), request.getStatus(),request.getCoinStatus(),request.getFuturesStatus());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateInviteFeeBackActivityResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" updateInviteFeeBackActivity exception:", e);
            observer.onError(e);
        }
    }

    @Override
    public void initInviteFeeBackActivity(InitInviteFeeBackActivityRequest request,
                                          StreamObserver<InitInviteFeeBackActivityResponse> observer) {
        InitInviteFeeBackActivityResponse response;
        try {
            response = inviteService.initInviteFeeBackActivity(request.getHeader().getOrgId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = InitInviteFeeBackActivityResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" initInviteFeeBackActivity exception:", e);
            observer.onError(e);
        }
    }

    /**
     *
     */
    @Override
    public void testInviteFeeBack(TestInviteFeeBackRequest request,
                                  StreamObserver<TestInviteFeeBackResponse> observer) {

        TestInviteFeeBackResponse response;
        try {
            response = inviteTaskService.testInviteFeeBack(request.getHeader().getOrgId(), request.getTime());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = TestInviteFeeBackResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" testInviteFeeBack exception:", e);
            observer.onError(e);
        }
    }


    @Override
    public void getAdminInviteBonusRecord(io.bhex.broker.grpc.invite.GetAdminInviteBonusRecordRequest request,
                                          io.grpc.stub.StreamObserver<io.bhex.broker.grpc.invite.GetAdminInviteBonusRecordResponse> observer) {

        GetAdminInviteBonusRecordResponse response;
        try {
            response = inviteService.getAdminInviteBonusRecord(request);
        } catch (Exception e) {
            response = GetAdminInviteBonusRecordResponse.getDefaultInstance();
            log.error(" getAdminInviteBonusRecord exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    @Override
    public void generateAdminInviteBonusRecord(io.bhex.broker.grpc.invite.GenerateAdminInviteBonusRecordRequest request,
                                               io.grpc.stub.StreamObserver<io.bhex.broker.grpc.invite.GenerateAdminInviteBonusRecordResponse> observer) {

        GenerateAdminInviteBonusRecordResponse response;
        try {
            response = inviteService.generateAdminInviteBonusRecord(request);
        } catch (Exception e) {
            response = GenerateAdminInviteBonusRecordResponse.newBuilder().setRet(-999).build();
            log.error(" generateAdminInviteBonusRecord exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    @Override
    public void executeAdminGrantInviteBonus(io.bhex.broker.grpc.invite.ExecuteAdminGrantInviteBonusRequest request,
                                             io.grpc.stub.StreamObserver<io.bhex.broker.grpc.invite.ExecuteAdminGrantInviteBonusResponse> observer) {
        ExecuteAdminGrantInviteBonusResponse response;
        try {
            response = inviteService.executeAdminGrantInviteBonus(request);
        } catch (Exception e) {
            response = ExecuteAdminGrantInviteBonusResponse.getDefaultInstance();
            log.error(" executeAdminGrantInviteBonus exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void updateInviteFeebackAutoTransfer(UpdateInviteFeebackAutoTransferRequest request,
                                                StreamObserver<UpdateInviteFeebackAutoTransferResponse> observer) {
        UpdateInviteFeebackAutoTransferResponse response;
        try {
            response = inviteService.updateInviteFeebackAutoTransfer(request.getOrgId(), request.getStatus());
        } catch (Exception e) {
            response = UpdateInviteFeebackAutoTransferResponse.getDefaultInstance();
            log.error(" updateInviteFeebackAutoTransfer exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void updateInviteFeebackPeriod(UpdateInviteFeebackPeriodRequest request,
                                          StreamObserver<UpdateInviteFeebackPeriodResponse> observer) {

        UpdateInviteFeebackPeriodResponse response;
        try {
            response = inviteService.updateInviteFeebackPeriod(request);
        } catch (Exception e) {
            response = UpdateInviteFeebackPeriodResponse.getDefaultInstance();
            log.error(" updateInviteFeebackPeriod exception:", e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getInviteBlackList(GetInviteBlackListRequest request,
                                   io.grpc.stub.StreamObserver<GetInviteBlackListResponse> observer) {
        GetInviteBlackListResponse response;
        try {
            response = inviteService.getInviteBlackList(request);
        } catch (Exception e) {
            response = GetInviteBlackListResponse.getDefaultInstance();
            log.error(" getInviteBlackList exception:", e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void addInviteBlackList(AddInviteBlackListRequest request,
                                   io.grpc.stub.StreamObserver<AddInviteBlackListResponse> observer) {

        AddInviteBlackListResponse response;
        try {
            response = inviteService.addInviteBlackList(request);
        } catch (Exception e) {
            response = AddInviteBlackListResponse.newBuilder().setRet(-999).build();
            log.error(" addInviteBlackList exception:", e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void deleteInviteBlackList(DeleteInviteBlackListRequest request,
                                      io.grpc.stub.StreamObserver<DeleteInviteBlackListResponse> observer) {
        DeleteInviteBlackListResponse response;
        try {
            response = inviteService.deleteInviteBlackList(request);
        } catch (Exception e) {
            response = DeleteInviteBlackListResponse.newBuilder().setRet(-999).build();
            log.error(" addInviteBlackList exception:", e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getInviteStatisticsRecordList(GetInviteStatisticsRecordListRequest request,
                                              StreamObserver<GetInviteStatisticsRecordListResponse> observer) {
        GetInviteStatisticsRecordListResponse response;
        try {
            response = inviteService.getInviteStatisticsRecordList(request);
        } catch (Exception e) {
            response = GetInviteStatisticsRecordListResponse.getDefaultInstance();
            log.error(" getInviteStatisticsRecordList exception:", e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getDailyTaskList(GetDailyTaskListRequest request,
                                 StreamObserver<GetDailyTaskListResponse> observer) {
        GetDailyTaskListResponse response;
        try {
            response = inviteService.getDailyTaskList(request);
        } catch (Exception e) {
            response = GetDailyTaskListResponse.getDefaultInstance();
            log.error(" getDailyTaskList exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getInviteCommonSetting(GetInviteCommonSettingRequest request,
                                       StreamObserver<GetInviteCommonSettingResponse> observer) {

        GetInviteCommonSettingResponse response;
        try {
            response = inviteService.getInviteCommonSetting(request.getOrgId());
        } catch (Exception e) {
            response = GetInviteCommonSettingResponse.getDefaultInstance();
            log.error(" getInviteCommonSetting exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void updateInviteCommonSetting(UpdateInviteCommonSettingRequest request,
                                          StreamObserver<UpdateInviteCommonSettingResponse> observer) {
        UpdateInviteCommonSettingResponse response;
        try {
            response = inviteService.updateInviteCommonSetting(request);
        } catch (Exception e) {
            response = UpdateInviteCommonSettingResponse.newBuilder().setRet(-999).build();
            log.error(" updateInviteCommonSetting exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void initInviteWechatConfig(InitInviteWechatConfigRequest request,
                                       StreamObserver<InitInviteWechatConfigResponse> observer) {
        InitInviteWechatConfigResponse response;
        try {
            inviteService.initInviteWechatConfig(request.getOrgId());
            response = InitInviteWechatConfigResponse.newBuilder().setRet(0).build();
        } catch (Exception e) {
            response = InitInviteWechatConfigResponse.newBuilder().setRet(-999).build();
            log.error(" updateInviteCommonSetting exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void cancelInviteRelation(CancelInviteRelationRequest request, StreamObserver<CancelInviteRelationResponse> observer) {
        CancelInviteRelationResponse response;
        try {
            inviteService.cancelInviteRelation(request);
            response = CancelInviteRelationResponse.newBuilder().setRet(0).build();
        } catch (Exception e) {
            response = CancelInviteRelationResponse.newBuilder().setRet(-999).build();
            log.error(" cancelInviteRelation exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void bindInviteRelation(BindInviteRelationRequest request, StreamObserver<BindInviteRelationResponse> observer) {
        BindInviteRelationResponse response;
        try {
            inviteService.bindInviteRelation(request);
            response = BindInviteRelationResponse.newBuilder().setRet(0).build();
        } catch (Exception e) {
            response = BindInviteRelationResponse.newBuilder().setRet(-999).build();
            log.error(" bindInviteRelation exception:", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }
}
