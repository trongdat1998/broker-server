package io.bhex.broker.server.grpc.server.service;


import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.protobuf.TextFormat;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;

import io.bhex.base.account.AccountType;
import io.bhex.base.account.BalanceDetailList;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.base.account.LockBalanceReply;
import io.bhex.base.account.LockBalanceRequest;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.base.account.UnlockBalanceRequest;
import io.bhex.base.account.UnlockBalanceResponse;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.BaseRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.server.service.po.ActivityTransfer;
import io.bhex.broker.server.grpc.server.service.po.ActivityTransferResult;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.LockBalanceLog;
import io.bhex.broker.server.model.UnlockBalanceLog;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.LockBalanceLogMapper;
import io.bhex.broker.server.primary.mapper.UnlockBalanceLogMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class BalanceService {

    @Autowired
    private GrpcBalanceService grpcBalanceService;

    @Autowired
    private AccountService accountService;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    private LockBalanceLogMapper lockBalanceLogMapper;

    @Resource
    private UnlockBalanceLogMapper unlockBalanceLogMapper;

    @Resource
    private AccountMapper accountMapper;

    public LockBalanceReply lockBalance(Long userId, Long orgId, String lockAmount, String tokenId, Long clientOrderId, String reason) {
        Long accountId = accountService.getAccountId(orgId, userId);
        if (accountId == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        LockBalanceRequest request = LockBalanceRequest
                .newBuilder()
                .setAccountId(accountId)
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(userId).build())
                .setClientReqId(clientOrderId)
                .setLockAmount(new BigDecimal(lockAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setTokenId(tokenId)
                .setLockedToPositionLocked(false)
                .setLockReason(reason)
                .build();

        LockBalanceReply reply = grpcBalanceService.lockBalance(request);
        if (reply == null || reply.getCode() != LockBalanceReply.ReplyCode.SUCCESS) {
        }
        log.info("lock success");
        return reply;
    }

    public UnlockBalanceResponse unLockBalance(Long userId, Long orgId, String unLockAmount, String tokenId, String reason, Long clientOrderId) {
        Long accountId = accountService.getAccountId(orgId, userId);
        if (accountId == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        UnlockBalanceRequest request = UnlockBalanceRequest
                .newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(userId).build())
                .setAccountId(accountId)
                .setClientReqId(clientOrderId)
                .setTokenId(tokenId)
                .setUnlockAmount(new BigDecimal(unLockAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setUnlockReason(reason)
                .build();

        //外层做逻辑处理 这个地方不需要抛异常 这样会影响后续的处理
        UnlockBalanceResponse response = grpcBalanceService.unLockBalance(request);
        if (response == null || response.getCode() != UnlockBalanceResponse.ReplyCode.SUCCESS) {
            log.warn("unlock fail accountId {} clientOrderId {} msg {}", accountId, clientOrderId, response.getMsg());
        }
        return response;
    }

    public UnlockBalanceResponse unLockBalanceByAccountId(Long userId, Long accountId, Long orgId, String unLockAmount, String tokenId, String reason, Long clientOrderId) {
        UnlockBalanceRequest request = UnlockBalanceRequest
                .newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(userId).build())
                .setAccountId(accountId)
                .setClientReqId(clientOrderId)
                .setTokenId(tokenId)
                .setUnlockAmount(new BigDecimal(unLockAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setUnlockReason(reason)
                .build();
        //外层做逻辑处理 这个地方不需要抛异常 这样会影响后续的处理
        UnlockBalanceResponse response = grpcBalanceService.unLockBalance(request);
        if (response == null || response.getCode() != UnlockBalanceResponse.ReplyCode.SUCCESS) {
            log.warn("unlock fail accountId {} clientOrderId {} msg {}", accountId, clientOrderId, response.getMsg());
        }
        return response;
    }

    /**
     * 从用户账户转到运营账户内
     */
    public SyncTransferResponse syncTransfer(Long userId, Long orgId, String amount, String tokenId, Integer fromPosition) {
        //fromPosition 0 从可用 非0从锁仓
        Long accountId = accountService.getAccountId(orgId, userId);
        if (accountId == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setClientTransferId(sequenceGenerator.getLong()) //from account
                .setSourceAccountId(accountId)
                .setSourceOrgId(orgId)
                .setSourceFlowSubject(BusinessSubject.ADMIN_TRANSFER)
                .setTokenId(tokenId)
                .setAmount(new BigDecimal(amount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setTargetAccountType(AccountType.OPERATION_ACCOUNT)//to account
                .setTargetOrgId(orgId)
                .setSourceFlowSubject(BusinessSubject.ADMIN_TRANSFER)
                .setFromPosition(fromPosition == 0 ? false : true)
                .build();
        SyncTransferResponse transferResponse = grpcBatchTransferService.syncTransfer(transferRequest);
        if (transferResponse.getCodeValue() != 200) {
            log.warn("syncTransfer fail transferRequest:{} msg:{}", TextFormat.shortDebugString(transferRequest), transferResponse.getMsg());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        log.info("syncTransfer success");
        return transferResponse;
    }


    public SyncTransferResponse syncTransferByAccountType(Long userId, Long orgId, String amount, String tokenId, Integer fromPosition) {
        //fromPosition 0 从可用 非0从锁仓
        Long accountId = accountService.getAccountId(orgId, userId);
        if (accountId == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setClientTransferId(sequenceGenerator.getLong()) //from account
                .setSourceAccountType(AccountType.OPERATION_ACCOUNT)
                .setSourceOrgId(orgId)
                .setSourceFlowSubject(BusinessSubject.ADMIN_TRANSFER)
                .setTokenId(tokenId)
                .setAmount(new BigDecimal(amount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setTargetAccountId(accountId)//to account
                .setTargetOrgId(orgId)
                .setSourceFlowSubject(BusinessSubject.ADMIN_TRANSFER)
                .setFromPosition(fromPosition == 0 ? false : true)
                .build();
        SyncTransferResponse transferResponse = grpcBatchTransferService.syncTransfer(transferRequest);
        if (transferResponse.getCodeValue() != 200) {
            log.warn("syncTransfer fail transferRequest:{} msg:{}", TextFormat.shortDebugString(transferRequest), transferResponse.getMsg());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        log.info("syncTransfer success");
        return transferResponse;
    }

    //批量锁仓落表接口 失败的拼装uid返回
    public String userLockBalanceForAdmin(Long orgId, String userStr, String amount, Integer type, String tokenId, String mark, Long adminId) {
        List<String> userIds = buildUserIdList(userStr);
        List<String> ids = new ArrayList<>();
        userIds.forEach(uid -> {
            Long clientOrderId = sequenceGenerator.getLong();
            Long accountId;
            try {
                accountId = accountService.getAccountId(orgId, Long.parseLong(uid));
            } catch (Exception ex) {
                ids.add(uid);
                return;
            }
            if (accountId == null || accountId == 0L) {
                ids.add(uid);
                return;
            }
            //存储原始数据状态 如果失败则记录下ID并返回 初始化状态
            LockBalanceLog lockBalanceLog = new LockBalanceLog();
            lockBalanceLog.setAccountId(accountId);
            lockBalanceLog.setAmount(new BigDecimal(amount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN));
            lockBalanceLog.setBrokerId(orgId);
            lockBalanceLog.setClientOrderId(clientOrderId);
            lockBalanceLog.setLastAmount(new BigDecimal(amount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN));
            lockBalanceLog.setStatus(0);
            lockBalanceLog.setSubjectType(1);
            lockBalanceLog.setTokenId(tokenId);
            lockBalanceLog.setUnlockAmount(BigDecimal.ZERO.setScale(ProtoConstants.PRECISION, RoundingMode.DOWN));
            lockBalanceLog.setUserId(Long.parseLong(uid));
            lockBalanceLog.setOperator(adminId != null ? adminId : 0L);
            lockBalanceLog.setCreateTime(new Date());
            lockBalanceLog.setUpdateTime(new Date());
            lockBalanceLog.setType(type);
            lockBalanceLog.setMark(mark);
            if (this.lockBalanceLogMapper.insertSelective(lockBalanceLog) != 1) {
                ids.add(uid);
                return;
            } else {
                if (lockBalanceNeedClientOrderId(Long.parseLong(uid), orgId, clientOrderId, accountId, tokenId, amount, mark)) {
                    //成功的话直接更新锁仓成功状态
                    if (lockBalanceLogMapper.updateUserLockBalanceLogStatus(orgId, Long.parseLong(uid), clientOrderId, 1) != 1) {
                        if (lockBalanceLogMapper.updateUserLockBalanceLogStatus(orgId, Long.parseLong(uid), clientOrderId, 1) != 1) {
                            log.error("userLockBalanceForAdmin fail orgId {} uid {} amount {} tokenId {} accountId {}", orgId, uid, amount, tokenId, accountId);
                        }
                    }
                } else {
                    //失败直接记录失败状态
                    lockBalanceLogMapper.updateUserLockBalanceLogStatus(orgId, Long.parseLong(uid), clientOrderId, 2);
                    ids.add(uid);
                }
            }
        });
        return ids.size() > 0 ? Joiner.on("|").join(ids) : "";
    }

    @Transactional(rollbackFor = Throwable.class)
    public String userUnLockBalanceForAdmin(Long lockId, Long orgId, Long userId, String amount, String mark, Long adminId) {
        Long accountId = accountService.getAccountId(orgId, userId);
        if (accountId == null || accountId == 0L) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }

        LockBalanceLog lockBalanceLog = this.lockBalanceLogMapper.queryLockBalanceLogByIdLock(lockId);
        if (lockBalanceLog == null || lockBalanceLog.getStatus() == 0 || lockBalanceLog.getStatus() == 2) {
            throw new BrokerException(BrokerErrorCode.USER_ACCOUNT_LOCK_TASK_STATUS_ILLEGAL);
        }

        if (!lockBalanceLog.getBrokerId().equals(orgId)) {
            throw new BrokerException(BrokerErrorCode.USER_ACCOUNT_LOCK_TASK_STATUS_ILLEGAL);
        }

        if (!lockBalanceLog.getUserId().equals(userId)) {
            throw new BrokerException(BrokerErrorCode.USER_ACCOUNT_LOCK_TASK_STATUS_ILLEGAL);
        }

        if (lockBalanceLog.getLastAmount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.USER_ACCOUNT_UNLOCK_AMOUNT_OVERSTEP_LIMIT);
        }

        if (lockBalanceLog.getLastAmount().compareTo(new BigDecimal(amount)) < 0) {
            throw new BrokerException(BrokerErrorCode.USER_ACCOUNT_UNLOCK_AMOUNT_OVERSTEP_LIMIT);
        }

        Long clientOrderId = sequenceGenerator.getLong();
        UnlockBalanceLog unlockBalanceLog = new UnlockBalanceLog();
        unlockBalanceLog.setLockId(lockId);
        unlockBalanceLog.setUserId(userId);
        unlockBalanceLog.setAccountId(accountId);
        unlockBalanceLog.setAmount(new BigDecimal(amount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN));
        unlockBalanceLog.setBrokerId(orgId);
        unlockBalanceLog.setOperator(adminId != null ? adminId : 0L);
        unlockBalanceLog.setStatus(1);
        unlockBalanceLog.setTokenId(lockBalanceLog.getTokenId());
        unlockBalanceLog.setType(lockBalanceLog.getType());
        unlockBalanceLog.setUpdateTime(new Date());
        unlockBalanceLog.setClientOrderId(clientOrderId);
        unlockBalanceLog.setCreateTime(new Date());
        unlockBalanceLog.setMark(mark);
        unlockBalanceLog.setSubjectType(0);
        if (this.unlockBalanceLogMapper.insertSelective(unlockBalanceLog) != 1) {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        if (lockBalanceLogMapper.updateUserUnLockBalanceLog(lockBalanceLog.getId(), new BigDecimal(amount)) != 1) {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        //解锁失败直接抛异常 回滚本地修改
        if (!unlockBalance(userId, orgId, clientOrderId, accountId, lockBalanceLog.getTokenId(), amount, mark)) {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
        return "";
    }

    private Boolean unlockBalance(Long userId, Long orgId, Long clientOrderId, Long accountId, String tokenId, String amount, String mark) {
        UnlockBalanceRequest request = UnlockBalanceRequest
                .newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(userId).build())
                .setAccountId(accountId)
                .setClientReqId(clientOrderId)
                .setTokenId(tokenId)
                .setUnlockAmount(new BigDecimal(amount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setUnlockReason(mark)
                .build();
        UnlockBalanceResponse response = UnlockBalanceResponse.newBuilder().build();
        try {
            response = grpcBalanceService.unLockBalance(request);
            if (response == null || response.getCode() != UnlockBalanceResponse.ReplyCode.SUCCESS) {
                return false;
            } else {
                return true;
            }
        } catch (Exception ex) {
            log.info("unlock fail orgId {} clientOrderId {} accountId {} tokenId {} amount {} msg {}",
                    orgId, clientOrderId, accountId, tokenId, amount, response.getMsg());
            return false;
        }
    }

    public Boolean lockBalanceNeedClientOrderId(Long userId, Long orgId, Long clientOrderId, Long accountId, String tokenId, String amount, String mark) {
        BaseRequest baseRequest = BaseRequest.newBuilder()
                .setOrganizationId(orgId)
                .setBrokerUserId(userId)
                .build();
        LockBalanceRequest request = LockBalanceRequest.newBuilder()
                .setBaseRequest(baseRequest)
                .setClientReqId(clientOrderId)
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setLockAmount(new BigDecimal(amount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setLockReason(mark)
                .build();

        LockBalanceReply reply = LockBalanceReply.newBuilder().build();
        try {
            reply = grpcBalanceService.lockBalance(request);
            if (reply.getCode() == LockBalanceReply.ReplyCode.SUCCESS) {
                return true;
            } else {
                log.info("lock fail orgId {} clientOrderId {} accountId {} tokenId {} amount {} msg {}  {}",
                        orgId, clientOrderId, accountId, tokenId, amount, reply.getCode(), reply.getMsg());
                return false;
            }
        } catch (Exception ex) {
            log.info("lock fail orgId {} clientOrderId {} accountId {} tokenId {} amount {} msg {}",
                    orgId, clientOrderId, accountId, tokenId, amount, reply.getMsg());
            return false;
        }
    }

    /**
     * A账户可用到B账户可用 B账户可用到A账户可用 A账户赠送到B账户锁仓或可用,从可用余额转账
     *
     * @return result
     */
    public ActivityTransferResult doActivityBuyAndAirdropAndLock(ActivityTransfer activityTransfer) {
        //抢购账户获得用户的币
        SyncTransferRequest syncTransferRequest = SyncTransferRequest.newBuilder()
                .setClientTransferId(activityTransfer.getIeoAccountClientOrderId())
                .setSourceOrgId(activityTransfer.getOrgId())
                .setSourceFlowSubject(BusinessSubject.TRANSFER)
                .setSourceAccountId(activityTransfer.getToAccountId())
                .setTokenId(activityTransfer.getSourceTokenId())
                .setAmount(new BigDecimal(activityTransfer.getSourceAmount()).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .setTargetAccountId(activityTransfer.getSourceAccountId())
                .setTargetOrgId(activityTransfer.getOrgId())
                .setTargetFlowSubject(BusinessSubject.TRANSFER)
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(syncTransferRequest);
        if (response == null || !SyncTransferResponse.ResponseCode.SUCCESS.equals(response.getCode())) {
            log.warn("Transfer fail resultMsg {} resultCode {}", response.getMsg(), response.getCodeValue());
            return ActivityTransferResult.builder().code(response.getCodeValue()).result(false).message(ActivityTransferResult.FAIL).build();
        }
        log.info("DoActivityBuyAndAirdropAndLock Transfer success resultMsg {} resultCode {}", response.getMsg(), response.getCodeValue());
        return ActivityTransferResult.builder().code(response.getCodeValue()).result(true).message(ActivityTransferResult.SUCCESS).build();
    }

    public List<LockBalanceLog> queryLockBalanceLogListByUserId(Long orgId, Long userId, Integer page, Integer size, Integer type) {
        int start = (page - 1) * size;
        if (userId == null || userId == 0L) {
            return lockBalanceLogMapper.queryLockBalanceLogList(orgId, start, size, type);
        } else {
            return lockBalanceLogMapper.queryLockBalanceLogListByUserId(orgId, userId, start, size, type);
        }
    }

    private List<String> buildUserIdList(String uid) {
        return Splitter.on('|').trimResults().omitEmptyStrings().splitToList(uid);
    }

    /**
     * VIP剩余bht按照23.887兑换比例 仅限作为bht兑换hbc使用！
     *
     * @param orgId           券商ID
     * @param userId          VIP UID
     * @param sourceAccountId 运营账户
     */
    public void lastBHTTransferToHbc(Long orgId, Long userId, Long sourceAccountId, String bhtTokenId, String hbcTokenId) {
        Account account = this.accountMapper.getMainAccount(orgId, userId);
        if (account == null) {
            log.info("lastBHTTransferToHbc orgId {} userId {} sourceAccountId {} account not find!", orgId, userId, sourceAccountId);
            return;
        }
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(account.getAccountId())
                .addTokenId(bhtTokenId)
                .build();
        BalanceDetailList balanceDetailList = grpcBalanceService.getBalanceDetail(request);
        BigDecimal available = BigDecimal.ZERO;
        if (balanceDetailList.getBalanceDetailsCount() > 0) {
            available = new BigDecimal(balanceDetailList.getBalanceDetails(0).getAvailable().getStr()).setScale(8, RoundingMode.DOWN);
        }

        if (available.compareTo(BigDecimal.ZERO) <= 0) {
            log.info("lastBHTTransferToHbc orgId {} userId {} sourceAccountId {} available = 0!", orgId, userId, sourceAccountId);
        }

        BigDecimal hbcAmount = available.divide(new BigDecimal("23.887"), 8, RoundingMode.DOWN).setScale(8, RoundingMode.DOWN);
        if (hbcAmount.compareTo(BigDecimal.ZERO) <= 0) {
            log.info("lastBHTTransferToHbc orgId {} userId {} sourceAccountId {} hbcAmount = 0!", orgId, userId, sourceAccountId);
            return;
        }

        log.info("lastBHTTransferToHbc do transfer ! orgId {} userId {} sourceAccountId {} available = {}!", orgId, userId, sourceAccountId, available.stripTrailingZeros().toPlainString());
        //运营账户获得BHT从用户手里转出
        SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setClientTransferId(sequenceGenerator.getLong())
                .setSourceOrgId(orgId)
                .setSourceFlowSubject(BusinessSubject.TRANSFER)
                .setSourceAccountId(account.getAccountId())
                .setFromPosition(false)
                .setTokenId(bhtTokenId)
                .setAmount(available.stripTrailingZeros().toPlainString())
                .setTargetAccountId(sourceAccountId) //IEO 活动账户
                .setTargetOrgId(orgId)
                .setTargetFlowSubject(BusinessSubject.TRANSFER)
                .build();
        SyncTransferResponse orgTransferResponse = grpcBatchTransferService.syncTransfer(transferRequest);
        if (orgTransferResponse.getCode() != SyncTransferResponse.ResponseCode.SUCCESS) {
            log.info("lastBHTTransferToHbc BHT to source fail orgId {} userId {} sourceAccountId {} code {} !", orgId, userId, sourceAccountId, orgTransferResponse.getCodeValue());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        //用户获得HBC 从指定账户转出
        SyncTransferRequest userTransferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setClientTransferId(sequenceGenerator.getLong())
                .setSourceOrgId(orgId)
                .setSourceFlowSubject(BusinessSubject.AIRDROP)
                .setSourceAccountId(sourceAccountId) //IEO 活动账户
                .setFromPosition(false)
                .setTokenId(hbcTokenId)
                .setAmount(hbcAmount.stripTrailingZeros().toPlainString())
                .setTargetAccountId(account.getAccountId())
                .setTargetOrgId(orgId)
                .setTargetFlowSubject(BusinessSubject.AIRDROP)
                .build();
        SyncTransferResponse userTransferResponse
                = grpcBatchTransferService.syncTransfer(userTransferRequest);
        if (userTransferResponse.getCode() != SyncTransferResponse.ResponseCode.SUCCESS) {
            log.info("lastBHTTransferToHbc HBC to user fail orgId {} userId {} sourceAccountId {} hbcAmount {} code {} !", orgId, userId, sourceAccountId, hbcAmount, userTransferResponse.getCodeValue());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }
}
