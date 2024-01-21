package io.bhex.broker.server.grpc.server.service;

import com.google.protobuf.TextFormat;
import io.bhex.base.account.*;
import io.bhex.base.proto.ErrorCode;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.finance_support.*;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.grpc.client.service.GrpcAccountService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.AccountFinance;
import io.bhex.broker.server.primary.mapper.AccountFinanceMapper;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 提供给财务使用的内部服务
 */
@Slf4j
@Service
public class FinanceSupportService {

    @Resource
    private AccountFinanceMapper accountFinanceMapper;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private GrpcAccountService grpcAccountService;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    public CreateFinanceAccountResponse createFinanceAccount(CreateFinanceAccountRequest request) {
        Long userId = request.getUserId();
        Long orgId = request.getOrgId();

        if (accountFinanceMapper.getByOrgId(orgId) != null) {
            throw new BrokerException(BrokerErrorCode.FINANCE_ACCOUNT_EXIST);
        }

        // 调用平台接口创建账户
        SimpleCreateAccountRequest createAccountReq = SimpleCreateAccountRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setOrgId(orgId)
                .setUserId(String.valueOf(userId))
                .setCountryCode("")
                .setMobile("")
                .setEmail("")
                .build();
        SimpleCreateAccountReply createAccountReply = grpcAccountService.createAccount(createAccountReq);
        if (createAccountReply.getErrCode() != ErrorCode.SUCCESS_VALUE) {
            log.error("createFinanceAccount invoke GrpcAccountService.createAccount error:{}", createAccountReply.getErrCode());
            throw new BrokerException(BrokerErrorCode.UNCAUGHT_EXCEPTION);
        }

        // 保存账户信息到broker的账户表中
        Long currentTimestamp = System.currentTimeMillis();

        // 保存主账户信息
        Long mainAccountId = createAccountReply.getAccountId();
        saveBrokerAccount(orgId, mainAccountId, AccountType.MAIN.value(), userId, currentTimestamp);

        // 保存期权账户信息
        Long optionAccountId = createAccountReply.getOptionAccountId();
        saveBrokerAccount(orgId, optionAccountId, AccountType.OPTION.value(), userId, currentTimestamp);

        // 保存期货账户信息
        Long futuresAccountId = createAccountReply.getFuturesAccountId();
        saveBrokerAccount(orgId, futuresAccountId, AccountType.FUTURES.value(), userId, currentTimestamp);

        // 保存财务用户在该券商下的主账户信息
        AccountFinance record = AccountFinance.builder()
                .orgId(orgId)
                .userId(userId)
                .accountId(mainAccountId)
                .accountIndex(0)
                .accountType(AccountType.MAIN.value())
                .createType(AccountFinance.CREATE_TYPE_AUTO)
                .build();
        accountFinanceMapper.insertSelective(record);

        // 构造响应信息
        AccountFinance accountFinance = accountFinanceMapper.getByOrgId(orgId);
        return CreateFinanceAccountResponse.newBuilder()
                .setRet(0)
                .setAccountInfo(toFinanceAccountInfo(accountFinance))
                .build();
    }

    public FinanceAccountTransferResponse financeAccountTransfer(FinanceAccountTransferRequest request) {
        AccountFinance sourceAccount = accountFinanceMapper.getByOrgId(request.getSourceOrgId());
        if (sourceAccount == null) {
            log.error("financeAccountTransfer: can not find account info with source orgId: {}", request.getSourceOrgId());
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        if (sourceAccount.getAccountId() != request.getSourceAccountId()) {
            log.error("financeAccountTransfer: source orgId: {} and source accountId: {} mismatch",
                    request.getSourceOrgId(), request.getSourceAccountId());
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        AccountFinance targetAccount = accountFinanceMapper.getByOrgId(request.getTargetOrgId());
        if (targetAccount == null) {
            log.error("financeAccountTransfer: can not find account info with target orgId: {}", request.getTargetOrgId());
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        if (targetAccount.getAccountId() != request.getTargetAccountId()) {
            log.error("financeAccountTransfer: target orgId: {} and target accountId: {} mismatch",
                    request.getTargetOrgId(), request.getTargetAccountId());
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        Long sourceOrgId = sourceAccount.getRealOrgId() == null ? sourceAccount.getOrgId() : sourceAccount.getRealOrgId();
        SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(sourceOrgId))
                .setClientTransferId(request.getClientTransferId())
                .setSourceAccountId(request.getSourceAccountId())
                .setSourceAccountType(io.bhex.base.account.AccountType.GENERAL_ACCOUNT)
                .setSourceOrgId(sourceOrgId)
                .setSourceFlowSubject(BusinessSubject.TRANSFER)
                .setTokenId(request.getTokenId())
                .setAmount(request.getAmount())
                .setTargetAccountId(request.getTargetAccountId())
                .setTargetAccountType(io.bhex.base.account.AccountType.GENERAL_ACCOUNT)
                .setTargetOrgId(request.getTargetOrgId())
                .setTargetFlowSubject(BusinessSubject.TRANSFER)
                .build();
        SyncTransferResponse transferResponse = grpcBatchTransferService.syncTransfer(transferRequest);
        if (transferResponse.getCodeValue() != 200) {
            log.error("financeAccountTransfer error. transferRequest:{} transferResponse: {}",
                    TextFormat.shortDebugString(transferRequest), TextFormat.shortDebugString(transferResponse));
            throw new BrokerException(BrokerErrorCode.BALANCE_TRANSFER_FAILED);
        }

        return FinanceAccountTransferResponse.newBuilder().setRet(0).build();
    }

    public QueryFinanceAccountsResponse queryFinanceAccounts(QueryFinanceAccountsRequest request) {
        QueryFinanceAccountsResponse.Builder responseBuilder = QueryFinanceAccountsResponse.newBuilder();

        if (request.getOrgId() > 0) {
            AccountFinance accountFinance = accountFinanceMapper.getByOrgId(request.getOrgId());
            if (accountFinance != null) {
                responseBuilder.addAccountInfo(toFinanceAccountInfo(accountFinance));
            }
        } else if (request.getAccountId() > 0) {
            AccountFinance accountFinance = accountFinanceMapper.getByAccountId(request.getAccountId());
            if (accountFinance != null) {
                responseBuilder.addAccountInfo(toFinanceAccountInfo(accountFinance));
            }
        } else {
            // 查询所有有效的账户信息
            List<AccountFinance> accountFinances = accountFinanceMapper.getAccountFinanceList();
            if (CollectionUtils.isNotEmpty(accountFinances)) {
                List<FinanceAccountInfo> accountInfos = accountFinances
                        .stream()
                        .map(this::toFinanceAccountInfo)
                        .collect(Collectors.toList());
                responseBuilder.addAllAccountInfo(accountInfos);
            }
        }

        return responseBuilder.setRet(0).build();
    }

    private FinanceAccountInfo toFinanceAccountInfo(AccountFinance accountFinance) {
        return FinanceAccountInfo.newBuilder()
                .setAccountId(accountFinance.getAccountId())
                .setAccountType(AccountTypeEnum.forNumber(accountFinance.getAccountType() - 1))
                .setAccountIndex(accountFinance.getAccountIndex())
                .setCreateType(AccountCreateType.forNumber(accountFinance.getCreateType()))
                .setId(accountFinance.getId())
                .setOrgId(accountFinance.getOrgId())
                .setUserId(accountFinance.getUserId())
                .build();
    }

    private void saveBrokerAccount(Long orgId, Long accountId, Integer accountType, Long userId, Long timestamp) {
        Account account = Account.builder()
                .orgId(orgId)
                .userId(userId)
                .accountId(accountId)
                .accountName("")
                .accountType(accountType)
                .created(timestamp)
                .updated(timestamp)
                .build();
        accountMapper.insertRecord(account);
    }
}
