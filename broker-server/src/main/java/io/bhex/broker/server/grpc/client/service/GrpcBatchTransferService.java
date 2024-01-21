package io.bhex.broker.server.grpc.client.service;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import io.bhex.base.account.BatchSyncTransferRequest;
import io.bhex.base.account.BatchTransferRequest;
import io.bhex.base.account.BatchTransferResponse;
import io.bhex.base.account.ConvertRequest;
import io.bhex.base.account.ConvertResponse;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.RiskControlBalanceService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BigDecimalUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcBatchTransferService extends GrpcBaseService {

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private AccountService accountService;

    @Resource
    private RiskControlBalanceService riskControlBalanceService;

    public BatchTransferResponse batchTransfer(BatchTransferRequest request) {
        try {
            request.getTransferToList().forEach(transfer -> {
                BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(transfer.getAmount()) ? transfer.getAmount() : "");
            });
            return grpcClientConfig.batchTransferServiceBlockingStub(BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest())).batchTransfer(request);
        } catch (StatusRuntimeException e) {
            if (e.getStatus() != null && "Insufficient balance".equalsIgnoreCase(e.getStatus().getDescription())) {
                log.warn("{}, request:{}", printStatusRuntimeException(e), request);
            } else {
                log.error("{}", printStatusRuntimeException(e), e);
            }
            throw commonStatusRuntimeException(e);
        }
    }

    public SyncTransferResponse syncTransfer(SyncTransferRequest request) {
        try {
            //检查转账账户是否通过风控拦截 如果被拦截抛出BrokerErrorCode.RISK_CONTROL_INTERCEPTION_LIMIT 业务方自行处理
            if (request.getSourceAccountId() > 0) {
                Account account = this.accountMapper.getAccountByAccountId(request.getSourceAccountId());
                if (account != null) {
                    riskControlBalanceService.checkRcBalance(request.getSourceOrgId(), account.getUserId());
//                    UserBizPermission userRiskControlConfig = riskControlService.query(request.getSourceOrgId(), account.getUserId());
//                    if (userRiskControlConfig != null && userRiskControlConfig.getAssetTransferOut() == 1) {
//                        throw new BrokerException(BrokerErrorCode.RISK_CONTROL_INTERCEPTION_LIMIT);
//                    }
                } else {
                    log.info("syncTransfer account is null orgId {} accountId {}", request.getSourceOrgId(), request.getSourceAccountId());
                }
            } else {
                Long userId = accountService.getUserIdByAccountType(request.getSourceOrgId(), request.getSourceAccountType().getNumber());
                log.info("syncTransfer getUserIdByAccountType orgId {} userId {} ", request.getSourceOrgId(), userId);
                riskControlBalanceService.checkRcBalance(request.getSourceOrgId(), userId);
//                UserBizPermission userRiskControlConfig = riskControlService.query(request.getSourceOrgId(), userId);
//                if (userRiskControlConfig != null && userRiskControlConfig.getAssetTransferOut() == 1) {
//                    throw new BrokerException(BrokerErrorCode.RISK_CONTROL_INTERCEPTION_LIMIT);
//                }
            }
            BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(request.getAmount()) ? request.getAmount() : "");
            return grpcClientConfig.batchTransferServiceBlockingStub(BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest())).syncTransfer(request);
        } catch (StatusRuntimeException e) {
            log.error("syncTransfer error {}", printStatusRuntimeException(e), e);
            throw commonStatusRuntimeException(e);
        } catch (BrokerException e) {
            log.info("syncTransfer error clientOrderId {} sourceOrgId {} sourceAccountId {} sourceAccountType {} targetOrgId {} targetAccountId {} targetAccountType {} tokenId {} amount {} error {}",
                    request.getClientTransferId(),
                    request.getSourceOrgId(),
                    request.getSourceAccountId(),
                    request.getSourceAccountType(),
                    request.getTargetOrgId(),
                    request.getTargetAccountId(),
                    request.getTargetAccountType(),
                    request.getTokenId(),
                    request.getAmount(), e);
            throw e;
        } catch (Exception ex) {
            log.info("syncTransfer fail {}", ex);
            throw ex;
        }
    }

    public SyncTransferResponse batchSyncTransfer(BatchSyncTransferRequest request) {
        try {
            request.getSyncTransferRequestListList().forEach(transfer -> {
                BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(transfer.getAmount()) ? transfer.getAmount() : "");
            });
            return grpcClientConfig.batchTransferServiceBlockingStub(BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest())).batchSyncTransfer(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e), e);
            throw commonStatusRuntimeException(e);
        }
    }

    public ConvertResponse convert(ConvertRequest request) {
        try {
            BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(request.getMakerAmount()) ? request.getMakerAmount() : "");
            BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(request.getTakerAmount()) ? request.getTakerAmount() : "");
            return grpcClientConfig.batchTransferServiceBlockingStub(BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest())).convert(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e), e);
            throw commonStatusRuntimeException(e);
        }
    }
}
