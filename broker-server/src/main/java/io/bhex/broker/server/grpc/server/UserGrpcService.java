/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/21
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import com.google.common.base.Strings;
import com.google.protobuf.TextFormat;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.TwoStepAuth;
import io.bhex.broker.grpc.user.User;
import io.bhex.broker.grpc.user.*;
import io.bhex.broker.server.grpc.server.service.NoticeTemplateService;
import io.bhex.broker.server.grpc.server.service.UserExtStatusService;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.model.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class UserGrpcService extends UserServiceGrpc.UserServiceImplBase {

    @Resource
    private UserService userService;
    @Resource
    private UserExtStatusService userExtStatusService;
    @Resource
    private NoticeTemplateService noticeTemplateService;

    @Override
    public void mobileRegister(MobileRegisterRequest request, StreamObserver<RegisterResponse> observer) {
        RegisterResponse response;
        try {
            response = userService.mobileRegister(request.getHeader(), request.getNationalCode(), request.getMobile(),
                    request.getPassword(), request.getOrderId(), request.getVerifyCode(), request.getInviteCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = RegisterResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("mobileRegister error", e);
            observer.onError(e);
        }
    }

    @Override
    public void emailRegister(EmailRegisterRequest request, StreamObserver<RegisterResponse> observer) {
        RegisterResponse response;
        try {
            response = userService.emailRegister(request.getHeader(), request.getEmail(),
                    request.getPassword(), request.getOrderId(), request.getVerifyCode(), request.getInviteCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = RegisterResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("emailRegister error", e);
            observer.onError(e);
        }
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> observer) {
        RegisterResponse response;
        try {
            response = userService.importRegister(request.getHeader(), request.getNationalCode(), request.getMobile(), request.getEmail(),
                    request.getPassword(), request.getInviteCode(), request.getInviteUserId(), request.getSendSuccessMsg(), request.getCheckInviteInfo(), true);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = RegisterResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("register error", e);
            observer.onError(e);
        }
    }

    @Override
    public void quickRegister(QuickRegisterRequest request, StreamObserver<RegisterResponse> observer) {
        RegisterResponse response;
        try {
            response = userService.quickRegister(request.getHeader(), request.getNationalCode(), request.getMobile(), request.getEmail(),
                    request.getOrderId(), request.getVerifyCode(), request.getInviteCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = RegisterResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("register error", e);
            observer.onError(e);
        }
    }

    @Override
    public void thirdPartySimpleRegister(ThirdPartySimpleRegisterRequest request, StreamObserver<RegisterResponse> observer) {
        RegisterResponse response;
        try {
            response = userService.registerWithThirdUserIdAndNoneUserInfo(request.getHeader(), request.getThirdUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = RegisterResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("thirdPartySimpleRegister error", e);
            observer.onError(e);
        }
    }

    @Override
    public void mobileLogin(MobileLoginRequest request, StreamObserver<LoginResponse> observer) {
        LoginResponse response;
        try {
            response = userService.mobileLogin(request.getHeader(), request.getNationalCode(), request.getMobile(),
                    request.getPassword(), request.getNeed2FaCheck(), request.getSendSuccessMsg());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = LoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("mobileLogin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void emailLogin(EmailLoginRequest request, StreamObserver<LoginResponse> observer) {
        LoginResponse response;
        try {
            response = userService.emailLogin(request.getHeader(), request.getEmail(), request.getPassword(),
                    request.getNeed2FaCheck(), request.getSendSuccessMsg());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = LoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("emailLogin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void login(LoginRequest request, StreamObserver<LoginResponse> observer) {
        LoginResponse response;
        try {
            LoginType loginType = request.getLoginType();
            if (loginType == LoginType.LOGIN_WITH_MOBILE) {
                response = userService.mobileLogin(request.getHeader(), request.getNationalCode(), request.getMobile(),
                        request.getPassword(), request.getNeed2FaCheck(), request.getSendSuccessMsg());
            } else {
                response = userService.emailLogin(request.getHeader(), request.getEmail(), request.getPassword(),
                        request.getNeed2FaCheck(), request.getSendSuccessMsg());
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = LoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("login error", e);
            observer.onError(e);
        }
    }

    @Override
    public void usernameLogin(UsernameLoginRequest request, StreamObserver<LoginResponse> observer) {
        LoginResponse response;
        try {
            response = userService.usernameLogin(request.getHeader(), request.getUsername().trim(), request.getPassword(),
                    request.getNeed2FaCheck(), request.getSendSuccessMsg());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = LoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("login error", e);
            observer.onError(e);
        }
    }

    @Override
    public void thirdPartySimpleLogin(ThirdPartySimpleLoginRequest request, StreamObserver<LoginResponse> observer) {
        LoginResponse response;
        try {
            response = userService.thirdPartyUserLogin(request.getHeader(), request.getThirdUserId(), request.getUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = LoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("thirdPartySimpleLogin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void loginAdvance(LoginAdvanceRequest request, StreamObserver<LoginAdvanceResponse> observer) {
        LoginAdvanceResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = userService.loginAdvance(request.getHeader(), request.getRequestId(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = LoginAdvanceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("loginAdvance error", e);
            observer.onError(e);
        }
    }

    @Override
    public void quickLogin(QuickLoginRequest request, StreamObserver<QuickLoginResponse> observer) {
        QuickLoginResponse response;
        try {
            response = userService.quickLogin(request.getHeader(), request.getNationalCode(), request.getMobile(), request.getEmail(),
                    request.getOrderId(), request.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QuickLoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("login error", e);
            observer.onError(e);
        }
    }

    @Override
    public void quickLoginCheckPassword(QuickLoginCheckPasswordRequest request, StreamObserver<QuickLoginCheckPasswordResponse> observer) {
        QuickLoginCheckPasswordResponse response;
        try {
            response = userService.quickLoginCheckPassword(request.getHeader(), request.getRequestId(), request.getPassword());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QuickLoginCheckPasswordResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("login error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getLoginQrCode(GetLoginQrCodeRequest request, StreamObserver<GetLoginQrCodeResponse> observer) {
        observer.onNext(GetLoginQrCodeResponse.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public void scanLoginQrCode(ScanLoginQrCodeRequest request, StreamObserver<ScanLoginQrCodeResponse> observer) {
        ScanLoginQrCodeResponse response;
        try {
            response = userService.scanLoginQrCode(request.getHeader(), request.getTicket(), request.getWebRequestHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ScanLoginQrCodeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("scanLoginQrCode error", e);
            observer.onError(e);
        }
    }

    @Override
    public void qrCodeAuthorizeLogin(QrCodeAuthorizeLoginRequest request, StreamObserver<QrCodeAuthorizeLoginResponse> observer) {
        QrCodeAuthorizeLoginResponse response;
        try {
            response = userService.qrCodeAuthorizeLogin(request.getHeader(), request.getTicket(), request.getAuthorizeLogin());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QrCodeAuthorizeLoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("qrCodeAuthorizeLogin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getScanLoginQrCodeResult(GetScanLoginQrCodeResultRequest request, StreamObserver<GetScanLoginQrCodeResultResponse> observer) {
        GetScanLoginQrCodeResultResponse response;
        try {
            response = userService.getScanLoginQrCodeResult(request.getHeader(), request.getTicket());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetScanLoginQrCodeResultResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getScanLoginQrCodeResult error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getUserInfo(GetUserInfoRequest request, StreamObserver<GetUserInfoResponse> observer) {
        GetUserInfoResponse response;
        try {
            response = userService.getUserInfo(request.getHeader().getOrgId(), request.getHeader().getUserId(), request.getMobile(), request.getEmail());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetUserInfoResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getUserInfo error", e);
            observer.onError(e);
        }
    }

    @Override
    public void editAntiPhishingCode(EditAntiPhishingCodeRequest request, StreamObserver<EditAntiPhishingCodeResponse> observer) {
        EditAntiPhishingCodeResponse response;
        try {
            response = userService.editAntiPhishingCode(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = EditAntiPhishingCodeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("editAntiPhishingCode error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryUserInfo(QueryUserInfoRequest request, StreamObserver<QueryUserInfoResponse> observer) {
//        CompletableFuture.runAsync(() -> {
            QueryUserInfoResponse response;
            try {
                Header header = request.getHeader();
                List<SimpleUserInfo> simpleUserInfoList = userService.querySimpleUserInfoList(header.getOrgId(), request.getSource(), request.getFromId(), request.getLastId(),
                        request.getStartTime(), request.getEndTime(), request.getLimit());
                List<User> responseUserList = simpleUserInfoList.stream()
                        .map(simpleUserInfo -> User.newBuilder()
                                .setUserId(simpleUserInfo.getUserId())
                                .setNationalCode(simpleUserInfo.getNationalCode())
                                .setMobile(simpleUserInfo.getMobile())
                                .setEmail(simpleUserInfo.getEmail())
                                .setRegisterType(simpleUserInfo.getRegisterType())
                                .setUserType(simpleUserInfo.getUserType())
                                .setVerifyStatus(simpleUserInfo.getVerifyStatus())
                                .setInviteUserId(simpleUserInfo.getInviteUserId())
                                .setSecondLevelInviteUserId(simpleUserInfo.getSecondLevelInviteUserId())
                                .setSource(simpleUserInfo.getSource())
                                .setRegisterDate(simpleUserInfo.getCreated())
                                .build()).collect(Collectors.toList());
                response = QueryUserInfoResponse.newBuilder().addAllUser(responseUserList).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryUserInfoResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryUserInfo error", e);
                observer.onError(e);
            }
//        });
    }

    @Override
    public void queryChangedUserInfo(QueryUserInfoRequest request, StreamObserver<QueryUserInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            QueryUserInfoResponse response;
            try {
                Header header = request.getHeader();
                List<SimpleUserInfo> simpleUserInfoList = userService.queryChangedSimpleUserInfoList(header.getOrgId(), request.getSource(), request.getFromId(), request.getLastId(),
                        request.getStartTime(), request.getEndTime(), request.getLimit());
                List<User> responseUserList = simpleUserInfoList.stream()
                        .map(simpleUserInfo -> User.newBuilder()
                                .setUserId(simpleUserInfo.getUserId())
                                .setNationalCode(simpleUserInfo.getNationalCode())
                                .setMobile(simpleUserInfo.getMobile())
                                .setEmail(simpleUserInfo.getEmail())
                                .setRegisterType(simpleUserInfo.getRegisterType())
                                .setUserType(simpleUserInfo.getUserType())
                                .setVerifyStatus(simpleUserInfo.getVerifyStatus())
                                .setInviteUserId(simpleUserInfo.getInviteUserId())
                                .setSecondLevelInviteUserId(simpleUserInfo.getSecondLevelInviteUserId())
                                .setSource(simpleUserInfo.getSource())
                                .setRegisterDate(simpleUserInfo.getCreated())
                                .setUserStatus(simpleUserInfo.getUserStatus())
                                .build()).collect(Collectors.toList());
                response = QueryUserInfoResponse.newBuilder().addAllUser(responseUserList).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryUserInfoResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryUserInfo error", e);
                observer.onError(e);
            }
        });
    }

    @Override
    public void queryUserInviteInfo(QueryUserInviteInfoRequest request, StreamObserver<QueryUserInviteInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            QueryUserInviteInfoResponse response;
            try {
                Header header = request.getHeader();
                List<UserInviteInfo> userInviteInfoList = userService.queryUserInviteInfo(header.getOrgId(), request.getUserId(), request.getInviteType(), request.getInvitedLeader(), request.getFromId(), request.getLastId(),
                        request.getStartTime(), request.getEndTime(), request.getSource(), request.getLimit());
                List<QueryUserInviteInfoResponse.InviteInfo> inviteInfoList = userInviteInfoList.stream()
                        .map(userInviteInfo -> QueryUserInviteInfoResponse.InviteInfo.newBuilder()
                                .setKycName(Strings.nullToEmpty(userInviteInfo.getKycName()))
                                .setInviteIndirectCount(userInviteInfo.getInviteIndirectVaildCount())
                                .setInviteId(userInviteInfo.getInviteId())
                                .setUserId(userInviteInfo.getUserId())
                                .setNationalCode(userInviteInfo.getNationalCode())
                                .setMobile(userInviteInfo.getMobile())
                                .setEmail(userInviteInfo.getEmail())
                                .setInviteType(userInviteInfo.getInviteType())
                                .setInvitedLeader(userInviteInfo.getInviteHobbitLeader() == 1)
                                .setVerifyStatus(userInviteInfo.getVerifyStatus())
                                .setRegisterType(userInviteInfo.getRegisterType())
                                .setRegisterTime(userInviteInfo.getCreated())
                                .setSource(userInviteInfo.getSource())
                                .build()).collect(Collectors.toList());
                response = QueryUserInviteInfoResponse.newBuilder().addAllInvite(inviteInfoList).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryUserInviteInfoResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryUserInviteInfo error", e);
                observer.onError(e);
            }
        });
    }

    @Override
    public void queryUserKycInfo(QueryUserKycInfoRequest request, StreamObserver<QueryUserKycInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            QueryUserKycInfoResponse response;
            try {
                List<UserVerify> userVerifyList = userService.queryUserKycInfoList(request.getHeader().getOrgId(), request.getFromId(), request.getLastId(),
                        request.getStartTime(), request.getEndTime(), request.getLimit());
                List<QueryUserKycInfoResponse.KycInfo> kycInfoList = userVerifyList.stream()
                        .map(userVerify -> QueryUserKycInfoResponse.KycInfo.newBuilder()
                                .setId(userVerify.getId())
                                .setUserId(userVerify.getUserId())
                                .setFirstName(userVerify.getFirstName())
                                .setSecondName(userVerify.getSecondName())
                                .setVerifyStatus(userVerify.getVerifyStatus())
                                .build())
                        .collect(Collectors.toList());
                observer.onNext(QueryUserKycInfoResponse.newBuilder().addAllKycInfo(kycInfoList).build());
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryUserKycInfoResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("rebuildInviteRelation error", e);
                observer.onError(e);
            }
        });
    }

    @Override
    public void rebuildInviteRelation(RebuildInviteRelationRequest request, StreamObserver<RebuildInviteRelationResponse> observer) {
        try {
            userService.rebuildUserInviteRelation(request.getHeader().getOrgId(), request.getHeader().getUserId(), request.getInviteUserId());
            observer.onNext(RebuildInviteRelationResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (BrokerException e) {
            RebuildInviteRelationResponse response = RebuildInviteRelationResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("rebuildInviteRelation error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getInviteCode(GetInviteCodeRequest request, StreamObserver<GetInviteCodeResponse> observer) {
        GetInviteCodeResponse response;
        try {
            response = userService.getInviteCode(request.getHeader(), request.getCodeUseLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetInviteCodeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getInviteCode error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getUserContact(GetUserContactRequest request, StreamObserver<GetUserContactResponse> observer) {
        GetUserContactResponse response;
        try {
            response = userService.getUserContact(request.getAccountId());
        } catch (Exception e) {
            log.error("getUserContact error {}", TextFormat.shortDebugString(request), e);
            response = GetUserContactResponse.getDefaultInstance();
        }

        observer.onNext(response);
        observer.onCompleted();
    }


    @Override
    public void queryLoginLog(QueryLoginLogRequest request, StreamObserver<QueryLoginLogResponse> observer) {
        QueryLoginLogResponse response;
        try {
            response = userService.queryLoginLog(request.getHeader(), request.getStartTime(),
                    request.getEndTime(), request.getFromId(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryLoginLogResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryLoginLog error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createFavorite(CreateFavoriteRequest request, StreamObserver<CreateFavoriteResponse> observer) {
        CreateFavoriteResponse response;
        try {
            response = userService.createFavorite(request.getHeader(), request.getExchangeId(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateFavoriteResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("createFavorite error", e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelFavorite(CancelFavoriteRequest request, StreamObserver<CancelFavoriteResponse> observer) {
        CancelFavoriteResponse response;
        try {
            response = userService.cancelFavorite(request.getHeader(), request.getExchangeId(), request.getSymbolId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CancelFavoriteResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancelFavorite error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryFavorites(QueryFavoritesRequest request, StreamObserver<QueryFavoritesResponse> observer) {
        QueryFavoritesResponse response;
        try {
            response = userService.queryFavorites(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryFavoritesResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryFavorites error", e);
            observer.onError(e);
        }
    }

    @Override
    public void sortFavorites(SortFavoritesRequest request, StreamObserver<SortFavoritesResponse> observer) {
        SortFavoritesResponse response;
        try {
            response = userService.sortFavorites(request.getHeader(), request.getFavoritesList());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SortFavoritesResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("sortFavorites error", e);
            observer.onError(e);
        }
    }

    @Override
    public void personalVerify(PersonalVerifyRequest request, StreamObserver<PersonalVerifyResponse> observer) {
        PersonalVerifyResponse response;
        try {
            TwoStepAuth auth = request.getTwoStepAuth();
            response = userService.userVerify(request.getHeader(), request.getNationality(), request.getFirstName().trim(), request.getSecondName().trim(),
                    request.getGender(), request.getCardType(), request.getCardNo(), request.getCardFrontUrl(), request.getCardHandUrl(),
                    auth.getAuthType(), auth.getOrderId(), auth.getVerifyCode());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = PersonalVerifyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("personalVerify error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getPersonalVerifyInfo(GetPersonalVerifyInfoRequest request, StreamObserver<GetPersonalVerifyInfoResponse> observer) {
        GetPersonalVerifyInfoResponse response;
        try {
            response = userService.getUserVerifyInfo(request.getHeader(), request.getWithDecryptIdNumber(), request.getWithDecryptPhotoUrl());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetPersonalVerifyInfoResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getPersonalVerifyInfo error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getUserContract(GetUserContractRequest request, StreamObserver<GetUserContractResponse> observer) {
        GetUserContractResponse response;
        try {
            response = userService.getUserContract(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetUserContractResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getUserContract error", e);
            observer.onError(e);
        }
    }

    @Override
    public void saveUserContract(SaveUserContractRequest request, StreamObserver<SaveUserContractResponse> observer) {
        SaveUserContractResponse response;
        try {
            response = userService.saveUserContract(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SaveUserContractResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("saveUserContract error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryUserExtStatus(QueryUserExtStatusRequest request, StreamObserver<QueryUserExtStatusResponse> observer) {
        QueryUserExtStatusResponse response;
        try {
            response = userExtStatusService.queryUserExtStatus(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryUserExtStatusResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryUserExtStatus error", e);
            observer.onError(e);
        }
    }

    @Override
    public void resetUserExtStatus(ResetUserExtStatusRequest request, StreamObserver<ResetUserExtStatusResponse> observer) {
        ResetUserExtStatusResponse response;
        try {
            response = userExtStatusService.resetUserExtStatus(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ResetUserExtStatusResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("resetUserExtStatus error", e);
            observer.onError(e);
        }
    }

    @Override
    public void userBalanceProof(UserBalanceProofRequest request, StreamObserver<UserBalanceProofResponse> observer) {
        UserBalanceProofResponse response;
        try {
            BalanceProof balanceProof = userService.getUserBalanceProof(request.getHeader(), request.getTokenId());
            response = UserBalanceProofResponse.newBuilder()
                    .setTokenId(balanceProof.getTokenId())
                    .setNonce(balanceProof.getNonce())
                    .setAmount(balanceProof.getAmount().stripTrailingZeros().toPlainString())
                    .setJson(balanceProof.getProofJson())
                    .setCreatedAt(balanceProof.getCreated())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UserBalanceProofResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("resetUserExtStatus error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getUserSettings(GetUserSettingsRequest request, StreamObserver<GetUserSettingsResponse> observer) {
        GetUserSettingsResponse response;
        try {
            UserSettings settings = userService.getUserSettings(request.getHeader());
            response = GetUserSettingsResponse.newBuilder()
                    .setCommonConfig(settings.getCommonConfig())
                    .setUpdated(settings.getUpdated())
                    .setCreated(settings.getCreated())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetUserSettingsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("resetUserExtStatus error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setUserSettings(SetUserSettingsRequest request, StreamObserver<SetUserSettingsResponse> observer) {
        SetUserSettingsResponse response;
        try {
            int ret = userService.setUserSettings(request.getHeader(), request.getCommonConfig());
            response = SetUserSettingsResponse.newBuilder()
                    .setRet(ret)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SetUserSettingsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("resetUserExtStatus error", e);
            observer.onError(e);
        }
    }

    @Override
    public void exploreApply(ExploreApplyRequest request, StreamObserver<ExploreApplyResponse> observer) {
        ExploreApplyResponse response;
        try {

            userService.exploreApply(request);

            response = ExploreApplyResponse.newBuilder()
                    .setRet(0)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ExploreApplyResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("exploreApply error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setAllowedTradingSymbols(SetAllowedTradingSymbolsRequest request, StreamObserver<SetAllowedTradingSymbolsResponse> observer) {
        SetAllowedTradingSymbolsResponse response;
        try {
            response = userExtStatusService.setAllowedTradingSymbols(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SetAllowedTradingSymbolsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setAllowedTradingSymbols error", e);
            observer.onError(e);
        }
    }

    @Override
    public void rcDisableUserTrade(RcDisableUserTradeRequest request, StreamObserver<RcDisableUserTradeResponse> observer) {
        RcDisableUserTradeResponse response;
        try {
            response = userExtStatusService.disableUserTrade(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = RcDisableUserTradeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("rcDisableUserTrade error", e);
            observer.onError(e);
        }
    }
}
