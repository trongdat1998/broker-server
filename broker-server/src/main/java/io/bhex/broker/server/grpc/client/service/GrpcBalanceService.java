/**********************************
 * @项目名称: broker-parent
 * @文件名称: io.bhex.broker.grpc.client
 * @Date 2018/6/25
 * @Author peiwei.ren@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.account.*;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BigDecimalUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcBalanceService extends GrpcBaseService {

    public BalanceDetailList getBalanceDetail(GetBalanceDetailRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.balanceServiceBlockingStub(orgId);
        try {
            return stub.getBalanceDetail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BalanceDetailList getBalanceDetailWithShortDeadline(GetBalanceDetailRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.shortDeadlineBalanceServiceBlockingStub(orgId);
        try {
            return stub.getBalanceDetail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public PositionResponseList getBalancePosition(GetPositionRequest request) {
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.balanceServiceBlockingStub(request.getBrokerId());
        try {
            return stub.getPosition(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BalanceDetailList queryUserBalanceByAccountType(GetBalanceDetailByAccountTypeRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.shortDeadlineBalanceServiceBlockingStub(orgId);
        try {
            return stub.getBalanceDetailByAccountType(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BalanceDetailList queryUserBalanceByAccountTypeWithShortDeadline(GetBalanceDetailByAccountTypeRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.shortDeadlineBalanceServiceBlockingStub(orgId);
        try {
            return stub.getBalanceDetailByAccountType(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public PositionResponseList getBalancePositionWithShortDeadline(GetPositionRequest request) {
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.shortDeadlineBalanceServiceBlockingStub(request.getBrokerId());
        try {
            return stub.getPosition(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BalanceFlowsReply queryBalanceFlow(GetBalanceFlowsWithPageRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.balanceServiceBlockingStub(orgId);
        try {
            return stub.getBalanceFlowsWithPage(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BalanceChangeResponse changeBalance(BalanceChangeRequest request) {
        BalanceChangeRecordServiceGrpc.BalanceChangeRecordServiceBlockingStub stub = grpcClientConfig.balanceChangeRecordStub(request.getOrgId());
        try {
            return stub.changeBalance(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // 获取期权资产列表  暂不分页
    public OptionAssetList getOptionAssetList(OptionAssetReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getOptionAssetList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // 获取期权资产列表  暂不分页
    public OptionAssetList getOptionAssetListWithShortDeadline(OptionAssetReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getOptionAssetList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // 获取期权钱包余额
    public OptionAccountDetailList getOptionAccountDetail(OptionAccountDetailReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.optionServerBlockingStub(orgId);
        try {
            return stub.getOptionAccountDetail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // 获取期权钱包余额
    public OptionAccountDetailList getOptionAccountDetailWithShortDeadline(OptionAccountDetailReq request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OptionServerGrpc.OptionServerBlockingStub stub = grpcClientConfig.shortDeadlineOptionServerBlockingStub(orgId);
        try {
            return stub.getOptionAccountDetail(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // 获取OTC可用资金
    public GetOtcAvailableResponse getOtcAvailable(GetOtcAvailableRequest request) {
        BalanceChangeRecordServiceGrpc.BalanceChangeRecordServiceBlockingStub stub = grpcClientConfig.balanceChangeRecordStub(request.getOrgId());
        try {
            return stub.getOtcAvailable(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // 锁仓
    public LockBalanceReply lockBalance(LockBalanceRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.balanceServiceBlockingStub(orgId);
        try {
            BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(request.getLockAmount()) ? request.getLockAmount() : "");
            return stub.lockBalance(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    // 解锁锁仓
    public UnlockBalanceResponse unLockBalance(UnlockBalanceRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientConfig.balanceServiceBlockingStub(orgId);
        try {
            BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(request.getUnlockAmount()) ? request.getUnlockAmount() : "");
            return stub.unlockBalance(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryBalanceReply queryColdWalletInfo(QueryBalanceRequest request) {
        ColdWalletServiceGrpc.ColdWalletServiceBlockingStub stub = grpcClientConfig.coldWalletServiceBlockingStub(request.getOrgId());
        try {
            return stub.queryBalance(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
