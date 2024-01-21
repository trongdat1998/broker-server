package io.bhex.broker.server.grpc.server;

import com.google.common.collect.Maps;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.grpc.admin.ListBannerReply;
import io.bhex.broker.grpc.user.level.*;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.grpc.server.service.CommonIniService;
import io.bhex.broker.server.grpc.server.service.UserLevelService;
import io.bhex.broker.server.grpc.server.service.WithdrawService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class UserLevelGrpcService extends UserLevelServiceGrpc.UserLevelServiceImplBase {

    @Resource
    private UserLevelService userLevelService;
    @Resource
    private WithdrawService withdrawService;
    @Resource
    private CommonIniService commonIniService;

    @Override
    public void userLevelConfig(UserLevelConfigRequest request, StreamObserver<UserLevelConfigResponse> observer) {
        UserLevelConfigResponse reply = userLevelService.userLevelConfig(request);
        observer.onNext(reply);
        observer.onCompleted();
    }

    @Override
    public void listUserLevelConfigs(ListUserLevelConfigsRequest request, StreamObserver<ListUserLevelConfigsResponse> observer) {
        BigDecimal needCheckIDCardQuotaConfig = withdrawService.getOrgNeedCheckIDCardQuotaQuantity(request.getOrgId());

        List<UserLevelConfigObj> objs = userLevelService.listUserLevelConfigs(request).stream()
                .map(c -> {
                    BigDecimal limitBtc = new BigDecimal(c.getWithdrawUpperLimitInBTC());
                    if (needCheckIDCardQuotaConfig.compareTo(limitBtc) > 0) {
                        return c.toBuilder().setWithdrawUpperLimitInBTC(needCheckIDCardQuotaConfig.stripTrailingZeros().toPlainString()).build();
                    }
                    return c;
                })
                .collect(Collectors.toList());
        ListUserLevelConfigsResponse reply = ListUserLevelConfigsResponse.newBuilder().addAllUserLevelConfig(objs).build();
        observer.onNext(reply);
        observer.onCompleted();
    }

    @Override
    public void deleteUserLevelConfig(DeleteUserLevelConfigRequest request, StreamObserver<DeleteUserLevelConfigResponse> observer) {
        DeleteUserLevelConfigResponse reply = userLevelService.deleteUserLevelConfig(request);
        observer.onNext(reply);
        observer.onCompleted();
    }

    @Override
    public void queryLevelConfigUsers(QueryLevelConfigUsersRequest request, StreamObserver<QueryLevelConfigUsersResponse> observer) {
        QueryLevelConfigUsersResponse reply = userLevelService.queryLevelConfigUsers(request);
        observer.onNext(reply);
        observer.onCompleted();
    }

    private String splitWithdrawAmountConfig(String config) {
        int lastDot = config.lastIndexOf(".");
        return config.substring(0, lastDot);
    }

    @Override
    public void getDefaultWithdrawConfig(GetDefaultWithdrawConfigRequest request, StreamObserver<GetDefaultWithdrawConfigResponse> observer) {
        long orgId = request.getOrgId();

        String firstWithdrawNeedKycQuota = commonIniService.getStringValueOrDefault(orgId, BrokerServerConstants.FIRST_WITHDRAW_NEED_KYC_CONFIG, "1.BTC");
        String withdrawNeedKycQuota = commonIniService.getStringValueOrDefault(orgId, BrokerServerConstants.WITHDRAW_NEED_KYC_CONFIG, "1.BTC");
        String firstWithdrawneedBrokerAuditQuota = commonIniService.getStringValueOrDefault(orgId, BrokerServerConstants.FIRST_WITHDRAW_BROKER_AUDIT_CONFIG, "1.BTC");
        String withdrawneedBrokerAuditQuota = commonIniService.getStringValueOrDefault(orgId, BrokerServerConstants.WITHDRAW_BROKER_AUDIT_CONFIG, "5.BTC");

        int withdrawCheckIdCardNoNumberLimit = commonIniService.getIntValueOrDefault(orgId, BrokerServerConstants.WITHDRAW_CHECK_ID_CARD_NO_NUMBER_CONFIG, 5);
        BigDecimal needCheckIDCardQuotaConfig = withdrawService.getOrgNeedCheckIDCardQuotaQuantity(orgId);

        Map<String, String> result = Maps.newHashMap();
        result.put("withdrawNeedKycQuota", splitWithdrawAmountConfig(withdrawNeedKycQuota));
        result.put("withdrawNumberLimit", withdrawCheckIdCardNoNumberLimit + "");
        result.put("withdrawUpperLimit", needCheckIDCardQuotaConfig.stripTrailingZeros().toPlainString());

        GetDefaultWithdrawConfigResponse reply = GetDefaultWithdrawConfigResponse.newBuilder().putAllConfig(result).build();
        observer.onNext(reply);
        observer.onCompleted();
    }

    @Override
    public void addWhiteListUsers(AddWhiteListUsersRequest request, StreamObserver<AddWhiteListUsersResponse> observer) {
        AddWhiteListUsersResponse reply = userLevelService.addWhiteListUsers(request);
        observer.onNext(reply);
        observer.onCompleted();
    }

    @Override
    public void deleteWhiteListUsers(DeleteWhiteListUsersRequest request, StreamObserver<DeleteWhiteListUsersResponse> responseObserver) {
        DeleteWhiteListUsersResponse reply = userLevelService.deleteWhiteListUsers(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryMyLevelConfig(QueryMyLevelConfigRequest request, StreamObserver<QueryMyLevelConfigResponse> observer) {
        QueryMyLevelConfigResponse reply = userLevelService.queryMyLevelConfig(request);
        String withdrawUpperLimitInBtc = reply.getWithdrawUpperLimitInBTC();

        //如果权益设置比系统默认的还小，就用系统的
        BigDecimal needCheckIDCardQuotaConfig = withdrawService.getOrgNeedCheckIDCardQuotaQuantity(request.getHeader().getOrgId());
        if (needCheckIDCardQuotaConfig.compareTo(new BigDecimal(withdrawUpperLimitInBtc)) > 0) {
            reply = reply.toBuilder().setWithdrawUpperLimitInBTC(needCheckIDCardQuotaConfig.stripTrailingZeros().toPlainString()).build();
        }

        observer.onNext(reply);
        observer.onCompleted();
    }


    @Override
    public void listAllUserLevelConfigs(ListAllUserLevelConfigsRequest request, StreamObserver<ListUserLevelConfigsResponse> observer) {
        BigDecimal needCheckIDCardQuotaConfig = withdrawService.getOrgNeedCheckIDCardQuotaQuantity(request.getHeader().getOrgId());

        List<UserLevelConfigObj> objs = userLevelService.listAllUserLevelConfigs(request).stream()
                .map(c -> {
                    BigDecimal limitBtc = new BigDecimal(c.getWithdrawUpperLimitInBTC());
                    if (needCheckIDCardQuotaConfig.compareTo(limitBtc) > 0) {
                        return c.toBuilder().setWithdrawUpperLimitInBTC(needCheckIDCardQuotaConfig.stripTrailingZeros().toPlainString()).build();
                    }
                    return c;
                })
                .collect(Collectors.toList());


        observer.onNext(ListUserLevelConfigsResponse.newBuilder().addAllUserLevelConfig(objs).build());
        observer.onCompleted();
    }
}
