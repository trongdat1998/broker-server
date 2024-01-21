package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.account.AccountType;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.broker.server.domain.FinanceTransferResult;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.model.FinanceRecord;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FinanceTransferService {

    @Autowired
    private GrpcBatchTransferService grpcBatchTransferService;

    public FinanceTransferResult purchaseTransfer(FinanceRecord record) {
        // 用户的钱 ----> 活期资金账户
        SyncTransferRequest request = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(record.getOrgId()))
                // 转账ID 、TOKEN 、数量
                .setClientTransferId(record.getTransferId())
                .setTokenId(record.getToken())
                .setAmount(record.getAmount().toPlainString())
                // 用户
                .setSourceOrgId(record.getOrgId())
                .setSourceAccountId(record.getAccountId())
                .setSourceAccountType(AccountType.GENERAL_ACCOUNT)
                .setSourceFlowSubject(BusinessSubject.BUY_WEALTH_MANAGEMENT_PRODUCTS)
                // 运营账户
                .setTargetOrgId(record.getOrgId())
                .setTargetAccountType(AccountType.CURRENT_DEPOSIT_BALANCE_ACCOUNT)
                .setTargetFlowSubject(BusinessSubject.BUY_WEALTH_MANAGEMENT_PRODUCTS)
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
        return this.convertFinanceTransferResult(response.getCode());
    }

    public FinanceTransferResult redeemTransfer(FinanceRecord record) {
        // 赎回账户 ----> 用户
        SyncTransferRequest request = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(record.getOrgId()))
                // 转账ID 、TOKEN 、数量
                .setClientTransferId(record.getTransferId())
                .setTokenId(record.getToken())
                .setAmount(record.getAmount().toPlainString())
                // 运营账户
                .setSourceOrgId(record.getOrgId())
                .setSourceAccountType(AccountType.PAY_PRINCIPAL_ACCOUNT)
                .setSourceFlowSubject(BusinessSubject.PRINCIPAL_WEALTH_MANAGEMENT_PRODUCTS)
                // 用户
                .setTargetOrgId(record.getOrgId())
                .setTargetAccountId(record.getAccountId())
                .setTargetAccountType(AccountType.GENERAL_ACCOUNT)
                .setTargetFlowSubject(BusinessSubject.PRINCIPAL_WEALTH_MANAGEMENT_PRODUCTS)
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
        return this.convertFinanceTransferResult(response.getCode());
    }

    public FinanceTransferResult interestTransfer(FinanceRecord record) {
        // 付息账户 ----> 用户
        SyncTransferRequest request = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(record.getOrgId()))
                // 转账ID 、TOKEN 、数量
                .setClientTransferId(record.getTransferId())
                .setTokenId(record.getToken())
                .setAmount(record.getAmount().toPlainString())
                // 运营账户
                .setSourceOrgId(record.getOrgId())
                .setSourceAccountType(AccountType.PAY_INTEREST_ACCOUNT)
                .setSourceFlowSubject(BusinessSubject.INTEREST_WEALTH_MANAGEMENT_PRODUCTS)
                // 用户
                .setTargetOrgId(record.getOrgId())
                .setTargetAccountId(record.getAccountId())
                .setTargetAccountType(AccountType.GENERAL_ACCOUNT)
                .setTargetFlowSubject(BusinessSubject.INTEREST_WEALTH_MANAGEMENT_PRODUCTS)
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
        return this.convertFinanceTransferResult(response.getCode());
    }

    public FinanceTransferResult convertFinanceTransferResult(SyncTransferResponse.ResponseCode code) {
        switch (code) {
            case SUCCESS:
                return FinanceTransferResult.SUCCESS;
            case FROM_ACCOUNT_NOT_EXIST:
                return FinanceTransferResult.FROM_ACCOUNT_NOT_EXIT;
            case TO_ACCOUNT_NOT_EXIST:
                return FinanceTransferResult.TO_ACCOUNT_NOT_EXIT;
            case BALANCE_INSUFFICIENT:
                return FinanceTransferResult.FROM_BALANCE_NOT_ENOUGH;
            case PROCESSING:
                return FinanceTransferResult.PROCESSING;
            default:
                return FinanceTransferResult.UNKNOWN_STATUS;
        }
    }

}
