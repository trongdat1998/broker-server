/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.grpc.client
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.account.*;
import io.bhex.base.clear.AssetRequest;
import io.bhex.base.clear.AssetResponse;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcAccountService extends GrpcBaseService {

    public SimpleCreateAccountReply createAccount(SimpleCreateAccountRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.simpleCreateAccount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BindAccountReply bindAccount(BindAccountRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.bindAccount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ModifyOrgUserEmailReply modifyEmail(ModifyOrgUserEmailRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.modifyOrgUserEmail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ModifyOrgUserMobileReply modifyMobile(ModifyOrgUserMobileRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.modifyOrgUserMobile(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetAccountByTypeReply getUserIdByAccountType(GetAccountByTypeRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.getAccountByType(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ModifyOrgUserReply asyncUserInfo(ModifyOrgUserRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.modifyOrgUser(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CreateOptionAccountReply createOptionAccount(Long orgId, Long userId) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(orgId);
        try {
            CreateOptionAccountRequest request = CreateOptionAccountRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setOrgId(orgId)
                    .setUserId(userId)
                    .build();
            return stub.createOptionAccount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CreateFuturesAccountReply createFuturesAccount(Long orgId, Long userId) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(orgId);
        try {
            CreateFuturesAccountRequest request = CreateFuturesAccountRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setOrgId(orgId)
                    .setUserId(userId)
                    .build();
            return stub.createFuturesAccount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // create sub account from bluehelix with params(org_id, user_id, account_type, index)
    public CreateSpecialUserAccountReply createSubAccount(CreateSpecialUserAccountRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.createSpecialUserAccount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SyncTransferResponse userAccountTransfer(UserAccountSafeTransferReq request) {
        UserAccountTransferGrpc.UserAccountTransferBlockingStub stub = grpcClientConfig.accountTransferServiceBlockingStub(request.getOrgId());
        try {
            return stub.userAccountSafeTransfer(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetCanTransferResp getCanTransferAmount(GetCanTransferReq request) {
        UserAccountTransferGrpc.UserAccountTransferBlockingStub stub = grpcClientConfig.accountTransferServiceBlockingStub(request.getOrgId());
        try {
            return stub.getCanTransferAmount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public AddAccountReply addAccount(AddAccountRequest request) {
        AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.accountServiceBlockingStub(request.getOrgId());
        try {
            return stub.addAccount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

/*    public AssetResponse getAsset(AssetRequest request) {
        io.bhex.base.clear.AccountServiceGrpc.AccountServiceBlockingStub stub = grpcClientConfig.getAccountServiceBlockingStub();
        try {
            return stub.getAsset(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }*/
}
