package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.base.account.*;
import io.bhex.base.proto.BaseRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.staking.EnumStakingTransfer;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.server.service.po.StakingTransferRecord;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 转账
 *
 * @author songxd
 * @date 2020-08-02
 */
@Slf4j
@Service
public class StakingTransferService {

    private final GrpcBatchTransferService grpcBatchTransferService;

    private final GrpcBalanceService balanceService;

    @Autowired
    public StakingTransferService(GrpcBalanceService balanceService, GrpcBatchTransferService grpcBatchTransferService) {
        this.balanceService = balanceService;
        this.grpcBatchTransferService = grpcBatchTransferService;
    }

    /**
     * transfer
     *
     * @param record     转账请求
     * @param retryCount 重试次数
     * @return
     */
    public EnumStakingTransfer transfer(StakingTransferRecord record, Integer retryCount) {
        SyncTransferRequest request = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(record.getOrgId()))
                // transfer id 、token 、amount
                .setClientTransferId(record.getTransferId())
                .setTokenId(record.getTokenId())
                .setAmount(record.getAmount().toPlainString())
                // set source account
                .setSourceOrgId(record.getSourceOrgId())
                .setSourceAccountId(record.getSourceAccountId())
                .setSourceAccountType(record.getSourceAccountType())
                .setSourceFlowSubject(record.getSourceBusinessSubject())
                // set target account
                .setTargetOrgId(record.getTargetOrgId())
                .setTargetAccountId(record.getTargetAccountId())
                .setTargetAccountType(record.getTargetAccountType())
                .setTargetFlowSubject(record.getTargetBusinessSubject())
                .build();

        // 未知：超时 OR 处理中 OR 异常
        EnumStakingTransfer enumStakingTransfer = EnumStakingTransfer.UNKNOWN;

        try {
            SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
            // 明确转账成功
            if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
                enumStakingTransfer = EnumStakingTransfer.SUCCESS;
            } else {
                // 有明确的错误
                if(response.getCodeValue() != SyncTransferResponse.ResponseCode.PROCESSING_VALUE){
                    enumStakingTransfer = EnumStakingTransfer.FAIL;
                }
                log.error("staking transfer failed error code: {}.params:{}", response.getCodeValue(), request.toString());
            }
        } catch (BrokerException e) {
            log.error("staking transfer failed params:{},e:{}", request.toString(), e);
            // 超时重试一次
            if (e.getCode() == BrokerErrorCode.GRPC_SERVER_TIMEOUT.code() && retryCount == 0) {
                transfer(record, ++retryCount);
            } else {
                enumStakingTransfer = EnumStakingTransfer.FAIL;
            }
        } catch (Exception e) {
            log.error("staking transfer failed params:{},e:{}", request.toString(), e);
            enumStakingTransfer = EnumStakingTransfer.FAIL;
        }
        return enumStakingTransfer;
    }

    /**
     * 锁仓
     *
     * @param record     转账记录
     * @param retryCount 重试次数
     * @return
     */
    public EnumStakingTransfer lockBalance(StakingTransferRecord record, Integer retryCount) {
        BaseRequest baseRequest = BaseRequest.newBuilder()
                .setOrganizationId(record.getOrgId())
                .setBrokerUserId(record.getUserId())
                .build();

        LockBalanceRequest request = LockBalanceRequest.newBuilder()
                .setBaseRequest(baseRequest)
                .setClientReqId(record.getTransferId())
                .setAccountId(record.getSourceAccountId())
                .setTokenId(record.getTokenId())
                .setLockAmount(record.getAmount().stripTrailingZeros().toPlainString())
                .setLockedToPositionLocked(false)
                .setLockReason("staking lock project")
                .build();

        // 未知：超时 OR 处理中 OR 异常
        EnumStakingTransfer enumStakingTransfer = EnumStakingTransfer.UNKNOWN;
        try {
            LockBalanceReply reply = balanceService.lockBalance(request);
            if (reply.getCode() == LockBalanceReply.ReplyCode.SUCCESS ||
                    reply.getCode() == LockBalanceReply.ReplyCode.REPEAT_LOCK) {
                enumStakingTransfer = EnumStakingTransfer.SUCCESS;
            } else {
                // 有明确的错误
                enumStakingTransfer = EnumStakingTransfer.FAIL;
                log.error("staking lock balance error. code: {}.", reply.getCodeValue());
            }
        } catch (BrokerException e) {
            log.error("staking lock balance error params:{},e:{}", request.toString(), e);
            // 超时重试一次
            if (e.getCode() == BrokerErrorCode.GRPC_SERVER_TIMEOUT.code() && retryCount == 0) {
                lockBalance(record, ++retryCount);
            } else {
                enumStakingTransfer = EnumStakingTransfer.FAIL;
            }
        } catch (Exception e) {
            log.error("staking lock balance error params:{},e:{}", request.toString(), e);
            enumStakingTransfer = EnumStakingTransfer.FAIL;
        }
        return enumStakingTransfer;
    }

    /**
     * 释放仓位
     *
     * @param record
     * @return
     */
    public EnumStakingTransfer unLockBalance(StakingTransferRecord record, Integer retryCount) {
        BaseRequest baseRequest = BaseRequest.newBuilder()
                .setOrganizationId(record.getOrgId())
                .setBrokerUserId(record.getUserId())
                .build();

        UnlockBalanceRequest request = UnlockBalanceRequest.newBuilder()
                .setBaseRequest(baseRequest)
                .setClientReqId(record.getTransferId())
                .setOriginClientReqId(record.getTransferId())
                .setAccountId(record.getSourceAccountId())
                .setTokenId(record.getTokenId())
                .setUnlockAmount(record.getAmount().stripTrailingZeros().toPlainString())
                .setUnlockFromPositionLocked(false)
                .setUnlockReason("staking lock project")
                .build();

        // 未知：超时 OR 处理中 OR 异常
        EnumStakingTransfer enumStakingTransfer = EnumStakingTransfer.UNKNOWN;
        try {
            UnlockBalanceResponse reply = balanceService.unLockBalance(request);
            if (reply.getCode() == UnlockBalanceResponse.ReplyCode.SUCCESS
                    || reply.getCode() == UnlockBalanceResponse.ReplyCode.REPEAT_UNLOCKED) {
                enumStakingTransfer = EnumStakingTransfer.SUCCESS;
            } else {
                enumStakingTransfer = EnumStakingTransfer.FAIL;
                log.error("staking unlock balance error. code: {}.params:{}", reply.getCodeValue(), request.toString());
            }
        } catch (BrokerException e) {
            log.error("staking unlock balance error params:{},e:{}", request.toString(), e);
            // 超时重试一次
            if (e.getCode() == BrokerErrorCode.GRPC_SERVER_TIMEOUT.code() && retryCount == 0) {
                unLockBalance(record, ++retryCount);
            } else {
                enumStakingTransfer = EnumStakingTransfer.FAIL;
            }
        } catch (Exception e) {
            log.error("staking unlock balance error params:{},e:{}", request.toString(), e);
            enumStakingTransfer = EnumStakingTransfer.FAIL;
        }
        return enumStakingTransfer;
    }
}
