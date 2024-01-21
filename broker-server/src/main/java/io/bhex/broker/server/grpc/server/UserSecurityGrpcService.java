/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/21
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.common.TwoStepAuth;
import io.bhex.broker.grpc.user.*;
import io.bhex.broker.server.grpc.server.service.UserSecurityService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class UserSecurityGrpcService extends UserSecurityServiceGrpc.UserSecurityServiceImplBase {

    @Resource
    private UserSecurityService userSecurityService;

    @Override
    public void bindMobile(BindMobileRequest request, StreamObserver<BindMobileResponse> observer) {
        BindMobileResponse response;
        try {
            response = userSecurityService.bindMobile(request.getHeader(), request.getNationalCode(), request.getMobile(),
                    request.getMobileOrderId(), request.getMobileVerifyCode(), request.getOrderId(), request.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BindMobileResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("bindMobile error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void bindEmail(BindEmailRequest request, StreamObserver<BindEmailResponse> observer) {
        BindEmailResponse response;
        try {
            response = userSecurityService.bindEmail(request.getHeader(), request.getEmail(),
                    request.getEmailOrderId(), request.getEmailVerifyCode(), request.getOrderId(), request.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BindEmailResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("bindEmail error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void beforeBindGA(BeforeBindGARequest request, StreamObserver<BeforeBindGAResponse> observer) {
        BeforeBindGAResponse response;
        try {
            response = userSecurityService.beforeBindGA(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BeforeBindGAResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("beforeBindGA error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void bindGA(BindGARequest request, StreamObserver<BindGAResponse> observer) {
        BindGAResponse response;
        try {
            response = userSecurityService.bindGA(request.getHeader(), request.getGaCode(), request.getOrderId(), request.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BindGAResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("bindGA error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    @Override
    public void bindGADirect(BindGADirectRequest request, StreamObserver<BindGAResponse> observer) {
        BindGAResponse response;
        try {
            response = userSecurityService.bindGADirect(request.getHeader(), request.getGaKey(), request.getGaCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BindGAResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("bindGADirect error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            observer.onError(e);
        }
    }

    public void unbindMobile(UnbindMobileRequest request, StreamObserver<UnbindMobileResponse> observer) {
        UnbindMobileResponse response;
        try {
            response = userSecurityService.unbindMobile(request.getHeader());
        } catch (BrokerException e) {
            response = UnbindMobileResponse.newBuilder().setRet(e.getCode()).build();
        } catch (Exception e) {
            response = UnbindMobileResponse.newBuilder().setRet(BrokerErrorCode.UNBIND_MOBILE_FAILED.code()).build();
            log.error(" unbindMobile exception:{}", TextFormat.shortDebugString(request), e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void unbindEmail(UnbindEmailRequest request, StreamObserver<UnbindEmailResponse> observer) {
        UnbindEmailResponse response;
        try {
            response = userSecurityService.unbindEmail(request.getHeader());
        } catch (BrokerException e) {
            response = UnbindEmailResponse.newBuilder().setRet(e.getCode()).build();
        } catch (Exception e) {
            response = UnbindEmailResponse.newBuilder().setRet(BrokerErrorCode.UNBIND_EMAIL_FAILED.code()).build();
            log.error(" unbindEmail exception:{}", TextFormat.shortDebugString(request), e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void unbindGA(UnbindGARequest request, StreamObserver<UnbindGAResponse> observer) {
        UnbindGAResponse response;
        try {
            response = userSecurityService.unbindGA(request.getHeader(), request.getHeader().getUserId(),
                    request.getUserOperation(), request.getTwoStepAuth());
        } catch (BrokerException e) {
            response = UnbindGAResponse.newBuilder().setRet(e.getCode()).build();
        } catch (Exception e) {
            response = UnbindGAResponse.newBuilder().setRet(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR.code()).build();
            log.error(" unbindGA exception:{}", TextFormat.shortDebugString(request), e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void mobileFindPwdCheck(MobileFindPwdCheckRequest request, StreamObserver<FindPwdCheckResponse> observer) {
        FindPwdCheckResponse response;
        try {
            response = userSecurityService.findPwdByMobileCheck(request.getHeader(), request.getNationalCode(), request.getMobile(),
                    request.getOrderId(), request.getVerifyCode(), request.getIsOldVersionRequest());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = FindPwdCheckResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("mobileFindPwdCheck error", e);
            observer.onError(e);
        }
    }

    @Override
    public void emailFindPwdCheck(EmailFindPwdCheckRequest request, StreamObserver<FindPwdCheckResponse> observer) {
        FindPwdCheckResponse response;
        try {
            response = userSecurityService.findPwdByEmailCheck(request.getHeader(), request.getEmail(),
                    request.getOrderId(), request.getVerifyCode(), request.getIsOldVersionRequest());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = FindPwdCheckResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("emailFindPwdCheck error", e);
            observer.onError(e);
        }
    }

    @Override
    public void sendFindPwdVerifyCode(SendFindPwdVerifyCodeRequest request, StreamObserver<SendFindPwdVerifyCodeResponse> observer) {
        SendFindPwdVerifyCodeResponse response;
        try {
            response = userSecurityService.sendFindPwdVerifyCode(request.getHeader(), request.getRequestId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SendFindPwdVerifyCodeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("sendFindPwdVerifyCode error", e);
            observer.onError(e);
        }
    }

    @Override
    public void findPwdCheck2(FindPwdCheck2Request request, StreamObserver<FindPwdCheck2Response> observer) {
        FindPwdCheck2Response response;
        try {
            userSecurityService.findPwdCheck2(request.getHeader(), request.getRequestId(),
                    request.getOrderId(), request.getVerifyCode());
            observer.onNext(FindPwdCheck2Response.getDefaultInstance());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = FindPwdCheck2Response.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("findPwdCheck2 error", e);
            observer.onError(e);
        }
    }

    @Override
    public void findPwd(FindPwdRequest request, StreamObserver<FindPwdResponse> observer) {
        FindPwdResponse response;
        try {
            response = userSecurityService.findPwd(request.getHeader(), request.getRequestId(), request.getPassword());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = FindPwdResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("findPwd error", e);
            observer.onError(e);
        }
    }

    @Override
    public void updatePassword(UpdatePasswordRequest request, StreamObserver<UpdatePasswordResponse> observer) {
        UpdatePasswordResponse response;
        try {
            response = userSecurityService.updatePassword(request.getHeader(), request.getOldPassword(), request.getNewPassword());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdatePasswordResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("updatePassword error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setLoginPassword(SetLoginPasswordRequest request, StreamObserver<SetLoginPasswordResponse> observer) {
        SetLoginPasswordResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = userSecurityService.setLoginPassword(request.getHeader(), request.getPassword(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SetLoginPasswordResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setLoginPassword error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createApiKey(CreateApiKeyRequest request, StreamObserver<CreateApiKeyResponse> observer) {
        CreateApiKeyResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = userSecurityService.createApiKey(request.getHeader(),
                    request.getAccountType(), request.getAccountIndex(), request.getTag(), request.getType(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createApiKey error", e);
            observer.onError(e);
        }
    }

    @Override
    public void updateApiKeyIps(UpdateApiKeyIpsRequest request, StreamObserver<UpdateApiKeyResponse> observer) {
        UpdateApiKeyResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = userSecurityService.updateApiKeyIps(request.getHeader(), request.getApiKeyId(), request.getIpWhiteList(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("updateApiKeyIps error", e);
            observer.onError(e);
        }
    }

    @Override
    public void updateApiKeyStatus(UpdateApiKeyStatusRequest request, StreamObserver<UpdateApiKeyResponse> observer) {
        UpdateApiKeyResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = userSecurityService.updateApiKeyStatus(request.getHeader(), request.getApiKeyId(), request.getStatus(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("updateApiKeyStatus error", e);
            observer.onError(e);
        }
    }

    @Override
    public void deleteApiKey(DeleteApiKeyRequest request, StreamObserver<DeleteApiKeyResponse> observer) {
        DeleteApiKeyResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = userSecurityService.deleteApiKey(request.getHeader(), request.getApiKeyId(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DeleteApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("deleteApiKey error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryApiKeys(QueryApiKeyRequest request, StreamObserver<QueryApiKeyResponse> observer) {
        QueryApiKeyResponse response;
        try {
            response = userSecurityService.queryApiKeys(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryApiKeys error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAuthorizedAccountApiKeys(QueryAuthorizedAccountApiKeysRequest request, StreamObserver<QueryApiKeyResponse> observer) {
        QueryApiKeyResponse response;
        try {
            response = userSecurityService.queryOrgAuthorizedAccountApiKeys(request.getHeader(), request.getAccountId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAuthorizedAccountApiKeys error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createThirdPartyUserApiKey(CreateThirdPartyUserApiKeyRequest request, StreamObserver<CreateApiKeyResponse> observer) {
        CreateApiKeyResponse response;
        try {
            BaseResult<CreateApiKeyResponse> result = userSecurityService.createThirdPartyUserApiKey(request.getHeader(), request.getThirdUserId(), request.getUserId(),
                    request.getAccountType(), request.getAccountIndex(), request.getTag(), request.getType());
            if (result.isSuccess()) {
                response = result.getData();
            } else {
                response = CreateApiKeyResponse.newBuilder().setRet(result.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createThirdPartyUserApiKey error", e);
            observer.onError(e);
        }
    }

    @Override
    public void updateThirdPartyUserApiKeyIps(UpdateThirdPartyUserApiKeyIpsRequest request, StreamObserver<UpdateApiKeyResponse> observer) {
        UpdateApiKeyResponse response;
        try {
            BaseResult<UpdateApiKeyResponse> result = userSecurityService.updateThirdPartyUserApiKeyIps(request.getHeader(), request.getThirdUserId(), request.getUserId(),
                    request.getApiKeyId(), request.getIpWhiteList());
            if (result.isSuccess()) {
                response = result.getData();
            } else {
                response = UpdateApiKeyResponse.newBuilder().setRet(result.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("updateThirdPartyUserApiKeyIps error", e);
            observer.onError(e);
        }
    }

    @Override
    public void updateThirdPartyUserApiKeyStatus(UpdateThirdPartyUserApiKeyStatusRequest request, StreamObserver<UpdateApiKeyResponse> observer) {
        UpdateApiKeyResponse response;
        try {
            BaseResult<UpdateApiKeyResponse> result = userSecurityService.updateThirdPartyUserApiKeyStatus(request.getHeader(), request.getThirdUserId(), request.getUserId(),
                    request.getApiKeyId(), request.getStatus());
            if (result.isSuccess()) {
                response = result.getData();
            } else {
                response = UpdateApiKeyResponse.newBuilder().setRet(result.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UpdateApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("updateThirdPartyUserApiKeyStatus error", e);
            observer.onError(e);
        }
    }

    @Override
    public void deleteThirdPartyUserApiKey(DeleteThirdPartyUserApiKeyRequest request, StreamObserver<DeleteApiKeyResponse> observer) {
        DeleteApiKeyResponse response;
        try {
            BaseResult<DeleteApiKeyResponse> result = userSecurityService.deleteThirdPartyUserApiKey(request.getHeader(), request.getThirdUserId(), request.getUserId(),
                    request.getApiKeyId());
            if (result.isSuccess()) {
                response = result.getData();
            } else {
                response = DeleteApiKeyResponse.newBuilder().setRet(result.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DeleteApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("deleteThirdPartyUserApiKey error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryThirdPartyUserApiKeys(QueryThirdPartyUserApiKeyRequest request, StreamObserver<QueryApiKeyResponse> observer) {
        QueryApiKeyResponse response;
        try {
            BaseResult<QueryApiKeyResponse> result = userSecurityService.queryThirdPartyUserApiKeys(request.getHeader(), request.getThirdUserId(), request.getUserId());
            if (result.isSuccess()) {
                response = result.getData();
            } else {
                response = QueryApiKeyResponse.newBuilder().setRet(result.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryApiKeyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryThirdPartyUserApiKeys error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setTradePassword(SetTradePasswordRequest request,
                                 StreamObserver<SetTradePasswordResponse> responseObserver) {
        SetTradePasswordResponse response;
        try {
            response = userSecurityService.setTradePassword(request.getHeader(), request.getSetType(), request.getTradePassword(),
                    request.getOrderId(), request.getVerifyCode(), request.getFrozenWithdraw());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SetTradePasswordResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setTradePassword error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void verifyTradePassword(VerifyTradePasswordRequest request,
                                    StreamObserver<VerifyTradePasswordResponse> responseObserver) {
        VerifyTradePasswordResponse response;
        try {
            response = userSecurityService.verifyTradePassword(request.getHeader(), request.getTradePassword());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = VerifyTradePasswordResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("verifyTradePassword error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void userUnbindGa(UserUnbindGaRequest request, StreamObserver<UserUnbindGaResponse> responseObserver) {
        UserUnbindGaResponse response;
        try {
            response = userSecurityService.userUnbindGa(request.getHeader(), request.getOrderId(), request.getVerifyCode(), request.getGaCode());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = UserUnbindGaResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("userUnbindGa error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void userUnbindMobile(UserUnbindMobileRequest request, StreamObserver<UserUnbindMobileResponse> responseObserver) {
        UserUnbindMobileResponse response;
        try {
            response = userSecurityService.userUnbindMobile(request.getHeader(), request.getEmailOrderId(), request.getEmailVerifyCode(), request.getMobileOrderId(), request.getMobileVerifyCode());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = UserUnbindMobileResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("userUnbindMobile error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void userUnbindEmail(UserUnbindEmailRequest request, StreamObserver<UserUnbindEmailResponse> responseObserver) {
        UserUnbindEmailResponse response;
        try {
            response = userSecurityService.userUnbindEmail(request.getHeader(), request.getEmailOrderId(), request.getEmailVerifyCode(), request.getMobileOrderId(), request.getMobileVerifyCode());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = UserUnbindEmailResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("userUnbindEmail error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void alterEmail(AlterEmailRequest request, StreamObserver<AlterEmailResponse> responseObserver) {
        AlterEmailResponse response;
        try {
            response = userSecurityService.alterEmail(request.getHeader(), request.getOriginalEmailOrderId(), request.getOriginalEmailVerifyCode(), request.getMobileOrderId(), request.getMobileVerifyCode(),
                    request.getEmail(), request.getAlterEmailOrderId(), request.getAlterEmailVerifyCode(),request.getGaVerifyCode());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AlterEmailResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("alterEmail error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void alterMobile(AlterMobileRequest request, StreamObserver<AlterMobileResponse> responseObserver) {
        AlterMobileResponse response;
        try {
            response = userSecurityService.alterMobile(request.getHeader(), request.getOriginalMobileOrderId(), request.getOriginalMobileVerifyCode(), request.getEmailOrderId(), request.getEmailVerifyCode(),
                    request.getNationalCode(), request.getMobile(), request.getAlterMobileOrderId(), request.getAlterMobileVerifyCode(),request.getGaVerifyCode());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AlterMobileResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("alterEmail error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void alterGa(AlterGaRequest request, StreamObserver<AlterGaResponse> responseObserver) {
        AlterGaResponse response;
        try {
            response = userSecurityService.alterGa(request.getHeader(),request.getOrderId(),request.getVerifyCode(),request.getOriginalGaCode(),request.getAlterGaCode());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AlterGaResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("alterGa error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void beforeAlterGa(BeforeAlterGaRequest request, StreamObserver<BeforeAlterGaResponse> responseObserver) {
        BeforeAlterGaResponse response;
        try {
            response = userSecurityService.beforeAlterGA(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = BeforeAlterGaResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            try {
                log.error("beforeAlterGa error:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException ex) {
                //
            }
            responseObserver.onError(e);
        }
    }
}
