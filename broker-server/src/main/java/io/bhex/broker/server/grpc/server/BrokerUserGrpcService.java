package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteListType;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.security.ApiKeyStatus;
import io.bhex.broker.grpc.security.SecurityUpdateApiKeyRequest;
import io.bhex.broker.grpc.security.SecurityUpdateApiKeyResponse;
import io.bhex.broker.grpc.user.GetPersonalVerifyInfoResponse;
import io.bhex.broker.grpc.user.PersonalVerifyInfo;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.grpc.client.service.GrpcSecurityService;
import io.bhex.broker.server.grpc.server.service.*;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.FrozenUserRecord;
import io.bhex.broker.server.model.LockBalanceLog;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class BrokerUserGrpcService extends BrokerUserServiceGrpc.BrokerUserServiceImplBase {

    @Resource
    private UserService userService;
    @Resource
    private WithdrawAddressService withdrawAddressService;
    @Resource
    private UserSecurityService userSecurityService;
    @Resource
    private GrpcSecurityService grpcSecurityService;
    @Resource
    private BalanceService balanceService;
    @Resource
    private AccountService accountService;

    @Override
    public void getBrokerUser(GetBrokerUserRequest request, StreamObserver<GetBrokerUserResponse> observer) {
        GetBrokerUserResponse response;
        try {
            GetBrokerUserResponse.Builder builder = GetBrokerUserResponse.newBuilder();


            User user = userService.getUserInfo(request.getOrgId(), request.getAccountId(), request.getUserId(),
                    request.getNationalCode(), request.getMobile(), request.getEmail());
            if (user != null) {
                if (!StringUtils.isEmpty(user.getMobile())) {
                    user.setMobile(user.getMobile());
                }
                if (!StringUtils.isEmpty(user.getEmail())) {
                    user.setEmail(user.getEmail());
                }

                BrokerUser.Builder brokerUser = BrokerUser.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(user, brokerUser);

                List<FrozenUserRecord> frozenRecords = userService.queryFrozenUserRecords(request.getOrgId(), request.getUserId());
                boolean freezeLogin = false;
                if(!CollectionUtils.isEmpty(frozenRecords)) {
                    for (FrozenUserRecord frozenRecord : frozenRecords) {
                        if (frozenRecord.getFrozenType() == FrozenType.FROZEN_LOGIN.type()) {
                            freezeLogin = true;
                        }
                        BrokerUser.FrozenInfo frozenInfo =  BrokerUser.FrozenInfo.newBuilder()
                                .setFrozenType(frozenRecord.getFrozenType())
                                .setFrozenReason(frozenRecord.getFrozenReason())
                                .setStartTime(frozenRecord.getStartTime())
                                .setEndTime(frozenRecord.getEndTime())
                                .build();
                        brokerUser.addFrozenInfo(frozenInfo);
                    }
                }

                brokerUser.setBindGa(user.getBindGA() == BrokerServerConstants.BIND_GA_SUCCESS.intValue());
                brokerUser.setIsFreezeLogin(freezeLogin ? 1 : 0);
                Header header = Header.newBuilder()
                        .setUserId(user.getUserId())
                        .setLanguage(request.getLanguage())
                        .build();
                GetPersonalVerifyInfoResponse kyc = userService.getUserVerifyInfo(header);
                PersonalVerifyInfo kycinfo = kyc.getVerifyInfo();
                if (kycinfo != null) {
                    brokerUser.setVerifyStatus(kycinfo.getVerifyStatus());
                    brokerUser.setFirstName(kycinfo.getFirstName());
                    brokerUser.setSecondName(kycinfo.getSecondName());
                }
                builder.setBrokerUser(brokerUser);
            }
            response = builder.build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            log.error("query broker user error", e);
            response = GetBrokerUserResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query broker user error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getUserIdByAccount(GetUserIdByAccountRequest request, StreamObserver<GetUserIdByAccountResponse> observer) {
        long userId = 0;
        try {
            User user = accountService.getUserInfoByAccountId(request.getOrgId(), request.getAccountId());
            userId = user.getUserId();
        } catch (Exception e) {
            log.info("accountId not found. {} {}", request.getOrgId(), request.getAccountId());
        }
        GetUserIdByAccountResponse response = GetUserIdByAccountResponse.newBuilder().setUserId(userId).build();
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void queryLoginLogs(QueryLoginLogsRequest request, StreamObserver<QueryLoginLogsResponse> observer) {

        QueryLoginLogsResponse response = userService.queryLoginLog4Admin(request.getUserId(), request.getStartTime(),
                request.getEndTime(), request.getCurrent(), request.getPageSize());
        observer.onNext(response);
        observer.onCompleted();

    }

    @Override
    public void getAccountInfos(GetAccountInfosRequest request, StreamObserver<GetAccountInfosResponse> observer) {
        GetAccountInfosResponse response;
        try {
            response = userService.getAccountByUserId(request.getOrgId(), request.getUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetAccountInfosResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void getWithdrawAddress(GetWithdrawAddressRequest request, StreamObserver<GetWithdrawAddressResponse> observer) {
        GetWithdrawAddressResponse response;
        try {
            response = withdrawAddressService.getWithdrawAddress(request.getUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetWithdrawAddressResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void unbindMobile(SecurityUnbindMobileRequest request, StreamObserver<SecurityUnbindMobileResponse> observer) {
        SecurityUnbindMobileResponse response;
        Header header = Header.newBuilder().setOrgId(request.getOrgId()).setUserId(request.getUserId()).build();
        try {
            log.info("admin operator:{} unbind mobile user:{}-{} GA", request.getAdminUserId(), request.getOrgId(), request.getUserId());
            userSecurityService.unbindMobile(header);
            response = SecurityUnbindMobileResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SecurityUnbindMobileResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("admin unbind mobile error", e);
            observer.onError(e);
        }
    }

    @Override
    public void unbindEmail(SecurityUnbindEmailRequest request, StreamObserver<SecurityUnbindEmailResponse> observer) {
        SecurityUnbindEmailResponse response;
        Header header = Header.newBuilder().setOrgId(request.getOrgId()).setUserId(request.getUserId()).build();
        try {
            log.info("admin operator:{} unbind email user:{}-{} GA", request.getAdminUserId(), request.getOrgId(), request.getUserId());
            userSecurityService.unbindEmail(header);
            response = SecurityUnbindEmailResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SecurityUnbindEmailResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("admin unbind email error", e);
            observer.onError(e);
        }
    }

    @Override
    public void unbindGA(SecurityUnbindGARequest request, StreamObserver<SecurityUnbindGAResponse> observer) {
        SecurityUnbindGAResponse response;
        Header header = Header.newBuilder().setOrgId(request.getOrgId()).setUserId(request.getUserId()).build();
        try {
            log.info("admin operator:{} unbind user:{}-{} GA", request.getAdminUserId(), request.getOrgId(), request.getUserId());
            userSecurityService.unbindGA(header, request.getUserId(), false, null);
            response = SecurityUnbindGAResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SecurityUnbindGAResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
            observer.onError(e);
        }
    }

    @Override
    public void disableUserLogin(DisableUserLoginRequest request, StreamObserver<DisableUserLoginResponse> observer) {

        //TODO 踢掉所有的登录
        DisableUserLoginResponse response;
        try {
            boolean updateSuc = userSecurityService.updateUserStatus(request.getUserId(), false);
            response = DisableUserLoginResponse.newBuilder().setRet(updateSuc ? 0 : 1).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = DisableUserLoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
            observer.onError(e);
        }
    }

    @Override
    public void reopenUserLogin(ReopenUserLoginRequest request, StreamObserver<ReopenUserLoginResponse> observer) {
        ReopenUserLoginResponse response;
        try {
            boolean updateSuc = userSecurityService.updateUserStatus(request.getUserId(), true);
            response = ReopenUserLoginResponse.newBuilder().setRet(updateSuc ? 0 : 1).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ReopenUserLoginResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void disableApiOperation(DisableApiOperationRequest request, StreamObserver<DisableApiOperationResponse> observer) {
        log.info("disableApiOperation request.{}", request);
        SecurityUpdateApiKeyResponse securityResponse = enableApi(request.getOrgId(),
                request.getUserId(), request.getAccountId(), false);
        log.info("disableApiOperation response.{}", securityResponse);
        DisableApiOperationResponse response = DisableApiOperationResponse.newBuilder().setRet(securityResponse.getRet()).build();
        observer.onNext(response);
        observer.onCompleted();
    }

    private SecurityUpdateApiKeyResponse enableApi(Long orgId, Long userId, Long accountId, Boolean enable) {
        Header header = Header.newBuilder()
                .setOrgId(orgId)
                .setUserId(userId)
                .build();
        SecurityUpdateApiKeyRequest apiKeyRequest = SecurityUpdateApiKeyRequest.newBuilder()
                .setHeader(header)
                .setUserId(userId)
                .setAccountId(accountId)
                .setStatus(enable ? ApiKeyStatus.ENABLE : ApiKeyStatus.FROZEN)
                .build();

        SecurityUpdateApiKeyResponse securityResponse = grpcSecurityService.updateApiKey(apiKeyRequest);
        return securityResponse;
    }

    @Override
    public void reopenApiOperation(ReopenApiOperationRequest request, StreamObserver<ReopenApiOperationResponse> observer) {
        log.info("reopenApiOperation request.{}", request);
        SecurityUpdateApiKeyResponse securityResponse = enableApi(request.getOrgId(),
                request.getUserId(), request.getAccountId(), false);
        log.info("reopenApiOperation response.{}", securityResponse);
        ReopenApiOperationResponse response = ReopenApiOperationResponse.newBuilder()
                .setRet(securityResponse.getRet())
                .build();
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void listUserAccount(ListUserAccountRequest request, StreamObserver<ListUserAccountResponse> observer) {
        ListUserAccountResponse response;
        try {
            response = userService.listUserAccount(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ListUserAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("listUserAccount error", e);
            observer.onError(e);
        }
    }


    @Override
    public void userLockBalanceForAdmin(UserLockRequest request, StreamObserver<UserLockResponse> observer) {
        UserLockResponse response;
        try {

            String message = balanceService.userLockBalanceForAdmin(request.getOrgId(), request.getUserIdStr(),
                    request.getAmount(), request.getType(), request.getTokenId(), request.getMark(), request.getAdminId());
            observer.onNext(UserLockResponse.newBuilder().setRet(200).setMsg(message).build());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UserLockResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("userLockBalanceForAdmin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void userUnLockBalanceForAdmin(UserUnlockRequest request, StreamObserver<UserUnlockResponse> observer) {
        UserUnlockResponse response;
        try {
            String message
                    = balanceService.userUnLockBalanceForAdmin(request.getLockId(), request.getOrgId(),
                    request.getUserId(), request.getAmount(), request.getMark(), request.getAdminId());
            observer.onNext(UserUnlockResponse.newBuilder().setRet(200).setMsg(message).build());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = UserUnlockResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("userUnLockBalanceForAdmin error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryLockBalanceLogListByUserId(QueryUserLockLogListRequest request, StreamObserver<QueryUserLockLogListResponse> observer) {
        QueryUserLockLogListResponse response;
        try {
            List<LockBalanceLog> lockBalanceLogs
                    = balanceService.queryLockBalanceLogListByUserId(request.getBrokerId(), request.getUserId(), request.getPage(), request.getSize(),request.getType());
            observer.onNext(QueryUserLockLogListResponse.newBuilder().addAllLockLog(buildLockBalanceLog(lockBalanceLogs)).build());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryUserLockLogListResponse.newBuilder().addAllLockLog(new ArrayList<>()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryLockBalanceLogListByUserId error", e);
            observer.onError(e);
        }
    }

    @Override
    public void checkBindFundAccount(CheckBindFundAccountRequest request, StreamObserver<CheckBindFundAccountResponse> observer) {

        try {
            CheckBindFundAccountResponse response = accountService.checkBindFundAccount(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("checkBindFundAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void bindFundAccount(BindFundAccountRequest request, StreamObserver<BindFundAccountResponse> observer) {

        try {
            BindFundAccountResponse response = accountService.bindFundAccount(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("bindFundAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryFundAccount(QueryFundAccountRequest request, StreamObserver<QueryFundAccountResponse> observer) {
        try {
            QueryFundAccountResponse response = accountService.queryFundAccount(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryFundAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setFundAccountShow(SetFundAccountShowRequest request, StreamObserver<SetFundAccountShowResponse> observer) {
        try {
            SetFundAccountShowResponse response = accountService.setFundAccountShow(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setFundAccountShow error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryFundAccountShow(QueryFundAccountShowRequest request, StreamObserver<QueryFundAccountShowResponse> observer) {
        try {
            QueryFundAccountShowResponse response = accountService.queryFundAccountShow(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryFundAccountShow error", e);
            observer.onError(e);
        }
    }

    private List<io.bhex.broker.grpc.admin.LockBalanceLog> buildLockBalanceLog(List<LockBalanceLog> lockBalanceLogs) {
        List<io.bhex.broker.grpc.admin.LockBalanceLog> lockBalanceLogList = new ArrayList<>();
        lockBalanceLogs.forEach(log -> {
            lockBalanceLogList.add(io.bhex.broker.grpc.admin.LockBalanceLog
                    .newBuilder()
                    .setType(log.getType())
                    .setAccountId(log.getAccountId())
                    .setAmount(log.getAmount().toPlainString())
                    .setBrokerId(log.getBrokerId())
                    .setId(log.getId())
                    .setSubjectType(log.getSubjectType())
                    .setClientOrderId(log.getClientOrderId())
                    .setLastAmount(log.getLastAmount().toPlainString())
                    .setUnlockAmount(log.getUnlockAmount().toPlainString())
                    .setMark(StringUtils.isEmpty(log.getMark()) ? "" : log.getMark())
                    .setTokenId(log.getTokenId())
                    .setUserId(log.getUserId())
                    .setCreateTime(String.valueOf(log.getCreateTime().getTime()))
                    .setUpdateTime(String.valueOf(log.getUpdateTime().getTime()))
                    .build());
        });
        return lockBalanceLogList;
    }


    @Override
    public void unfreezeUser(UnfreezeUserRequest request, StreamObserver<UnfreezeUserResponse> observer) {
        try {
            userService.unfreezeUsers(request.getOrgId(), request.getUserId(), request.getType());
            observer.onNext(UnfreezeUserResponse.newBuilder().setRet(0).build());
            observer.onCompleted();
        } catch (BrokerException e) {
            observer.onNext(UnfreezeUserResponse.newBuilder().setRet(e.getCode()).setMsg(e.getMessage()).build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }


    @Resource
    private UserBlackWhiteListConfigService userBlackWhiteListConfigService;

    @Resource
    private BaseConfigService baseConfigService;

    @Override
    public void getRcList(GetRcListRequest request, StreamObserver<GetRcListResponse> observer) {
        //rc_type withdraw.blacklist withdraw.whitelist
        List<GetRcListResponse.Item> items;
        if (request.getRcType().equals("disable.userlogin")) {
            List<User> list = userService.getUsersByStatus(request.getBrokerId(), request.getFromId(),
                    UserStatusEnum.DISABLED.getValue(), request.getPageSize(), request.getUserId());
            items = list.stream().map(c -> {
                GetRcListResponse.Item.Builder builder = GetRcListResponse.Item.newBuilder();
                builder.setUserId(c.getUserId());
                builder.setCreated(c.getUpdated());
                builder.setId(c.getId());
                builder.setEmail(c.getEmail());
                if (!StringUtils.isEmpty(c.getMobile())) {
                    builder.setMobile("+" + c.getNationalCode() + " " + c.getMobile());
                }
                return builder.build();
            }).collect(Collectors.toList());
            observer.onNext(GetRcListResponse.newBuilder().addAllItems(items).build());
            observer.onCompleted();
            return;
        }

        if (request.getRcType().equals("withdraw.blacklist") || request.getRcType().equals("withdraw.whitelist")) {
            UserBlackWhiteType type = request.getRcType().equals("withdraw.blacklist") ? UserBlackWhiteType.BLACK_CONFIG : UserBlackWhiteType.WHITE_CONFIG;

            List<UserBlackWhiteListConfig> configs = userBlackWhiteListConfigService.getBlackWhiteConfigs(request.getBrokerId(),
                    UserBlackWhiteListType.WITHDRAW_BLACK_WHITE_LIST_TYPE_VALUE, type.getNumber(), request.getPageSize(), request.getFromId(), request.getUserId());
            items = configs.stream().map(c -> {
                GetRcListResponse.Item.Builder builder = GetRcListResponse.Item.newBuilder();
                builder.setUserId(c.getUserId());
                builder.setCreated(c.getUpdated());
                builder.setId(c.getId());
                builder.setRemark(c.getReason());
                return builder.build();
            }).collect(Collectors.toList());
        } else {
            List<BaseConfigInfo> configs = baseConfigService.getUserConfigs(request.getBrokerId(), request.getRcType(),
                    request.getPageSize(), request.getFromId(), request.getUserId());
            items = configs.stream().map(c -> {

                GetRcListResponse.Item.Builder builder = GetRcListResponse.Item.newBuilder();
                builder.setUserId(Long.parseLong(c.getConfKey()));
                builder.setCreated(c.getUpdated());
                builder.setId(c.getId());

                return builder.build();
            }).collect(Collectors.toList());
        }
        if (CollectionUtils.isEmpty(items)) {
            observer.onNext(GetRcListResponse.newBuilder().build());
            observer.onCompleted();
            return;
        }

        List<Long> userIds = items.stream().map(i -> i.getUserId()).distinct().collect(Collectors.toList());
        List<User> users = userService.getUsers(userIds);
        Map<Long, User> userMap = new HashMap<>();
        users.forEach(u -> {
            userMap.put(u.getUserId(), u);
        });

        List<GetRcListResponse.Item> result = items.stream().map(i -> {
            GetRcListResponse.Item.Builder builder = i.toBuilder();
            User user = userMap.get(i.getUserId());
            if (user != null) {
                builder.setEmail(user.getEmail());
                if (!StringUtils.isEmpty(user.getMobile())) {
                    builder.setMobile("+" + user.getNationalCode() + " " + user.getMobile());
                }
            }
            return builder.build();
        }).collect(Collectors.toList());

        observer.onNext(GetRcListResponse.newBuilder().addAllItems(result).build());
        observer.onCompleted();
    }

    @Override
    public void userBatchUnLockAirDropForAdmin(UserBatchUnLockAirDropRequest request, StreamObserver<UserBatchUnLockAirDropResponse> responseObserver) {
        try {
            userService.batchUserUnlockAirDrop(request.getOrgId(),request.getUnlockType(),request.getTokenId(),request.getUserIds(),request.getMark());
            responseObserver.onNext(UserBatchUnLockAirDropResponse.newBuilder().setCode(200).build());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            responseObserver.onNext(UserBatchUnLockAirDropResponse.newBuilder().setCode(e.getCode()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminUpdateEmail(AdminUpdateEmailRequest request, StreamObserver<AdminUpdateEmailResponse> responseObserver) {
        AdminUpdateEmailResponse response;
        try {
            log.info("admin operator:{} update email user:{}-{}-{} ", request.getAdminUserId(), request.getOrgId(), request.getUserId(), request.getEmail());
            response = userSecurityService.adminUpdateEmail(request.getOrgId(), request.getUserId(), request.getEmail());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminUpdateEmailResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("admin update email error", e);
            responseObserver.onError(e);
        }
    }
}
