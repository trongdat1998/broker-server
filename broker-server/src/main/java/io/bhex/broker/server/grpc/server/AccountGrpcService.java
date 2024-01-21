/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import io.bhex.base.account.BusinessSubject;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.account.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.order.QueryPlanSpotOrdersResponse;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.BalanceBatchTransferType;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.ThirdPartyUserService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.BalanceBatchOperatePositionTask;
import io.bhex.broker.server.model.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class AccountGrpcService extends AccountServiceGrpc.AccountServiceImplBase {

    @Resource
    private AccountService accountService;

    @Resource(name = "orgRequestHandleTaskExecutor")
    private TaskExecutor orgRequestHandleTaskExecutor;

    @Resource(name = "userRequestHandleTaskExecutor")
    private TaskExecutor userRequestHandleTaskExecutor;

    @Resource
    private ThirdPartyUserService thirdPartyUserService;

    @Override
    public void queryAccounts(QueryAccountRequest request, StreamObserver<QueryAccountResponse> observer) {
        QueryAccountResponse response;
        try {
            response = accountService.queryAccount(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void getAccount(GetAccountRequest request, StreamObserver<GetAccountResponse> observer) {
        GetAccountResponse response;
        try {
            response = accountService.getAccount(request.getHeader(), request.getAccountType(), request.getAccountIndex());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getAvailableBalance(GetAvailableBalanceRequest request, StreamObserver<GetAvailableBalanceResponse> observer) {
        GetAvailableBalanceResponse response;
        try {
            String availableBalance = accountService.getAvailableBalance(request.getHeader(), request.getAccountType(), request.getAccountIndex(), request.getTokenId());
            response = GetAvailableBalanceResponse.newBuilder().setAvailable(availableBalance).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetAvailableBalanceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getAvailableBalance error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createSubAccount(CreateSubAccountRequest request, StreamObserver<CreateSubAccountResponse> observer) {
        CreateSubAccountResponse response;
        try {
            Account account = accountService.createSubAccount(request.getHeader(), AccountType.fromAccountTypeEnum(request.getAccountType()), request.getAuthorizedOrg(), request.getDesc());
            SubAccount subAccount = SubAccount.newBuilder()
                    .setOrgId(account.getOrgId())
                    .setUserId(account.getUserId())
                    .setAccountType(account.getAccountType())
                    .setIndex(account.getAccountIndex())
                    .setAccountId(account.getAccountId())
                    .setAccountName(account.getAccountName())
                    .setStatus(account.getAccountStatus())
                    .setAuthorizedOrg(account.getIsAuthorizedOrg() == 1)
                    .build();
            response = CreateSubAccountResponse.newBuilder().setSubAccount(subAccount).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateSubAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("create subAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void querySubAccount(QuerySubAccountRequest request, StreamObserver<QuerySubAccountResponse> observer) {
        QuerySubAccountResponse response;
        try {
            List<SubAccount> subAccountList = querySubAccountList(request.getHeader(), AccountType.fromAccountTypeEnum(request.getAccountType()));
            response = QuerySubAccountResponse.newBuilder().addAllSubAccount(subAccountList).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QuerySubAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query subAccount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAllSubAccount(QueryAllSubAccountRequest request, StreamObserver<QuerySubAccountResponse> observer) {
        QuerySubAccountResponse response;
        try {
            List<SubAccount> subAccountList = querySubAccountList(request.getHeader(), null);
            response = QuerySubAccountResponse.newBuilder().addAllSubAccount(subAccountList).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QuerySubAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query subAccount error", e);
            observer.onError(e);
        }
    }

    private List<SubAccount> querySubAccountList(Header header, AccountType accountType) {
        List<Account> accountList = null;
        if (accountType != null) {
            accountList = accountService.querySubAccountList(header, accountType);
        } else {
            accountList = accountService.queryAllSubAccount(header);
        }
        return accountList.stream()
                .map(account -> SubAccount.newBuilder()
                        .setOrgId(account.getOrgId())
                        .setUserId(account.getUserId())
                        .setAccountId(account.getAccountId())
                        .setAccountType(account.getAccountType())
                        .setIndex(account.getAccountIndex())
                        .setAccountName(account.getAccountName())
                        .setStatus(account.getAccountStatus())
                        .setIsForbid(account.getIsForbid())
                        .setForbidStartTime(account.getForbidStartTime())
                        .setForbidEndTime(account.getForbidEndTime())
                        .setAuthorizedOrg(account.getIsAuthorizedOrg() == 1)
                        .build())
                .collect(Collectors.toList());
    }

    @Override
    public void subAccountTransfer(SubAccountTransferRequest request, StreamObserver<SubAccountTransferResponse> observer) {
        SubAccountTransferResponse response;
        try {
            accountService.userAccountTransfer(request.getHeader(), request.getFromAccountType(), request.getFromIndex(),
                    request.getToAccountType(), request.getToIndex(), request.getToken(), new BigDecimal(request.getAmount()));
            observer.onNext(SubAccountTransferResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SubAccountTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("subAccount balance transfer error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getCanTransferAmount(GetCanTransferRequest request, StreamObserver<GetCanTransferAmountResp> observer) {
        GetCanTransferAmountResp response;
        try {
            response = accountService.getCanTransferAmount(request.getHeader(), request.getAccountType(), request.getAccountIndex(), request.getTokenId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetCanTransferAmountResp.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getCanTransferAmount error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryBalance(QueryBalanceRequest request, StreamObserver<QueryBalanceResponse> observer) {
        QueryBalanceResponse response;
        try {
            response = accountService.queryBalance(request.getHeader(), request.getAccountType(), request.getAccountIndex(), request.getTokenIdsList());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryBalanceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryBalanceFlow(QueryBalanceFlowRequest request, StreamObserver<QueryBalanceFlowResponse> responseObserver) {
        CompletableFuture.runAsync(() -> {
            ServerCallStreamObserver<QueryBalanceFlowResponse> observer = (ServerCallStreamObserver<QueryBalanceFlowResponse>) responseObserver;
            observer.setCompression("gzip");
            QueryBalanceFlowResponse response;
            try {
                response = accountService.queryBalanceFlow(request.getHeader(), request.getAccountType(), request.getAccountIndex(),
                        request.getTokenIdsList(), request.getBalanceFlowTypesList(), request.getFromId(), request.getEndId(),
                        request.getStartTime(), request.getEndTime(), request.getLimit(), request.getQueryFromEs());
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = QueryBalanceFlowResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error("queryBalanceFlow error", e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getBrokerAccountList(GetBrokerAccountListRequest request,
                                     StreamObserver<GetBrokerAccountListResponse> observer) {
        GetBrokerAccountListResponse response;
        try {
            response = accountService.getBrokerAccountList(request.getHeader().getOrgId(), request.getFromId(),
                    request.getBeginTime(), request.getEndTime(), request.getLimit());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerAccountListResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getBrokerAccountList exception:{}", request, e);
            observer.onError(e);
        }
    }

    /**
     *
     */
    @Override
    public void verifyBrokerAccount(VerifyBrokerAccountRequest request,
                                    StreamObserver<VerifyBrokerAccountResponse> observer) {
        VerifyBrokerAccountResponse response;
        try {
            response = accountService.verifyBrokerAccount(request.getHeader().getOrgId(), request.getAccountIdsList());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = VerifyBrokerAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" verifyBrokerAccount exception:{}", request, e);
            observer.onError(e);
        }
    }

    @Override
    public void getBrokerAccountCount(GetBrokerAccountCountRequest request,
                                      StreamObserver<GetBrokerAccountCountResponse> observer) {
        GetBrokerAccountCountResponse response;
        try {
            response = accountService.getBrokerAccountCount(request.getHeader().getOrgId(), request.getBeginTime(), request.getEndTime());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBrokerAccountCountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" verifyBrokerAccount exception:{}", request, e);
            observer.onError(e);
        }
    }

    @Override
    public void getOptionAssetList(OptionAssetRequest request, StreamObserver<OptionAssetListResponse> observer) {
        CompletableFuture.runAsync(() -> {
            OptionAssetListResponse response;
            try {
                response = accountService.getOptionAssetList(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = OptionAssetListResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error(" getOptionAssetList exception:{}", request, e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getOptionAccountDetail(OptionAccountDetailRequest request, StreamObserver<OptionAccountDetailListResponse> observer) {
        CompletableFuture.runAsync(() -> {
            OptionAccountDetailListResponse response;
            try {
                response = accountService.getOptionAccountDetail(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = OptionAccountDetailListResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error(" getOptionAccountDetail exception:{}", request, e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getAllAssetInfo(GetAllAssetInfoRequest request, StreamObserver<GetAllAssetInfoResponse> observer) {
        CompletableFuture.runAsync(() -> {
            GetAllAssetInfoResponse response;
            try {
                response = accountService.getAllAssetInfo(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = GetAllAssetInfoResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error(" getAllAssetInfo exception:{}", request, e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void getOptionTradeable(GetOptionTradeableRequest request, StreamObserver<GetOptionTradeableResponse> observer) {
        CompletableFuture.runAsync(() -> {
            GetOptionTradeableResponse response;
            try {
                response = accountService.getOptionTradeAble(request);
                observer.onNext(response);
                observer.onCompleted();
            } catch (BrokerException e) {
                response = GetOptionTradeableResponse.newBuilder().setRet(e.getCode()).build();
                observer.onNext(response);
                observer.onCompleted();
            } catch (Exception e) {
                log.error(" getOptionTradeable exception:{}", request, e);
                observer.onError(e);
            }
        }, request.getHeader().getPlatform() == Platform.ORG_API ? orgRequestHandleTaskExecutor : userRequestHandleTaskExecutor);
    }

    @Override
    public void accountAuthorizedOrgTransfer(AccountAuthorizedOrgTransferRequest request, StreamObserver<AccountAuthorizedOrgTransferResponse> observer) {
        AccountAuthorizedOrgTransferResponse response;
        try {
            BalanceTransferObj transferObj = request.getBalanceTransfer();
            BaseResult baseResult = accountService.accountAuthorizedOrgTransfer(request.getHeader(), request.getClientOrderId(),
                    transferObj.getSourceUserId(), transferObj.getSourceAccountId(), transferObj.getTargetUserId(), transferObj.getTargetAccountId(),
                    transferObj.getTokenId(), transferObj.getAmount(),
                    transferObj.getFromSourceLock(), transferObj.getToTargetLock(), transferObj.getBusinessType(), transferObj.getSubBusinessType());
            if (baseResult.isSuccess()) {
                response = AccountAuthorizedOrgTransferResponse.getDefaultInstance();
            } else {
                response = AccountAuthorizedOrgTransferResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AccountAuthorizedOrgTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("accountAuthorizedOrgTransfer error", e);
            response = AccountAuthorizedOrgTransferResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void balanceTransfer(BalanceTransferRequest request, StreamObserver<BalanceTransferResponse> observer) {
        BalanceTransferResponse response;
        try {
            BalanceTransferObj transferObj = request.getBalanceTransfer();
            BaseResult baseResult = accountService.balanceTransfer(request.getHeader(), request.getClientOrderId(),
                    transferObj.getSourceUserId(), transferObj.getTargetUserId(), transferObj.getTokenId(), transferObj.getAmount(),
                    transferObj.getFromSourceLock(), transferObj.getToTargetLock(), transferObj.getBusinessType(), transferObj.getSubBusinessType(),
                    transferObj.getSourceAccountId(), transferObj.getTargetAccountId());
            if (baseResult.isSuccess()) {
                response = BalanceTransferResponse.getDefaultInstance();
            } else {
                response = BalanceTransferResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BalanceTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("balanceTransfer error", e);
            response = BalanceTransferResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void balanceBatchTransfer(BalanceBatchTransferRequest request, StreamObserver<BalanceBatchTransferResponse> observer) {
        BalanceBatchTransferResponse response;
        try {
            List<BalanceTransferObj> transferObjList = request.getTransferObjList();
            List<BalanceBatchTransfer> batchTransferList = transferObjList.stream()
                    .map(transferObj -> BalanceBatchTransfer.builder()
                            .sourceUserId(transferObj.getSourceUserId())
                            .sourceAccountType(0)
                            .sourceAccountId(transferObj.getSourceAccountId())
                            .targetUserId(transferObj.getTargetUserId())
                            .targetAccountType(0)
                            .targetAccountId(transferObj.getTargetAccountId())
                            .tokenId(transferObj.getTokenId())
                            .amount(new BigDecimal(transferObj.getAmount()))
                            .fromSourceLock(transferObj.getFromSourceLock() ? 1 : 0)
                            .toTargetLock(transferObj.getToTargetLock() ? 1 : 0)
                            .subject(transferObj.getBusinessType())
                            .subSubject(transferObj.getSubBusinessType())
                            .build())
                    .collect(Collectors.toList());
            BalanceBatchTransferTask transferTask = accountService.balanceBatchTransfer(request.getHeader(), request.getClientOrderId(), request.getType(),
                    request.getDesc(), batchTransferList);
            response = BalanceBatchTransferResponse.newBuilder()
                    .setTask(BalanceTransferTask.newBuilder()
                            .setId(transferTask.getId())
                            .setClientOrderId(transferTask.getClientOrderId())
                            .setDesc(transferTask.getDesc())
                            .setStatus(transferTask.getStatus())
                            .setCreated(transferTask.getCreated())
                            .setUpdated(transferTask.getUpdated())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BalanceBatchTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("balanceBatchTransfer error", e);
            response = BalanceBatchTransferResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_BATCH_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void airDrop(AirDropRequest request, StreamObserver<AirDropResponse> observer) {
        AirDropResponse response;
        try {
            List<BalanceTransferObj> transferObjList = request.getTransferObjList();
            List<BalanceBatchTransfer> batchTransferList = transferObjList.stream()
                    .map(transferObj -> BalanceBatchTransfer.builder()
                            .sourceUserId(0L)
                            .sourceAccountType(io.bhex.base.account.AccountType.OPERATION_ACCOUNT.getNumber())
                            .sourceAccountId(0L)
                            .targetUserId(transferObj.getTargetUserId())
                            .targetAccountType(0)
                            .targetAccountId(transferObj.getTargetAccountId())
                            .tokenId(transferObj.getTokenId())
                            .amount(new BigDecimal(transferObj.getAmount()))
                            .fromSourceLock(transferObj.getFromSourceLock() ? 1 : 0)
                            .toTargetLock(transferObj.getToTargetLock() ? 1 : 0)
                            .subject(BusinessSubject.AIRDROP_VALUE)
                            .subSubject(transferObj.getSubBusinessType())
                            .build())
                    .collect(Collectors.toList());
            BalanceBatchTransferTask transferTask = accountService.balanceBatchTransfer(request.getHeader(), request.getClientOrderId(),
                    BalanceBatchTransferType.AIR_DROP.type(), request.getDesc(), batchTransferList);
            response = AirDropResponse.newBuilder()
                    .setTask(BalanceTransferTask.newBuilder()
                            .setId(transferTask.getId())
                            .setClientOrderId(transferTask.getClientOrderId())
                            .setDesc(transferTask.getDesc())
                            .setStatus(transferTask.getStatus())
                            .setCreated(transferTask.getCreated())
                            .setUpdated(transferTask.getUpdated())
                            .build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AirDropResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("airDrop error", e);
            response = AirDropResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_BATCH_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void getBalanceBatchTransfer(GetBalanceBatchTransferRequest request, StreamObserver<GetBalanceBatchTransferResponse> observer) {
        GetBalanceBatchTransferResponse response;
        try {
            BalanceBatchTransferResult transferResult = accountService.getBalanceBatchTransferResult(request.getHeader(), request.getClientOrderId(), request.getType());
            BalanceBatchTransferTask transferTask = transferResult.getTransferTask();
            List<BalanceBatchTransfer> transferList = transferResult.getTransferList();
            List<BalanceTransferObj> transferObjList = new ArrayList<>();
            if (!CollectionUtils.isEmpty(transferList)) {
                transferObjList = transferList.stream()
                        .map(transfer -> BalanceTransferObj.newBuilder()
                                .setSourceUserId(transfer.getSourceUserId())
                                .setTargetUserId(transfer.getTargetUserId())
                                .setTokenId(transfer.getTokenId())
                                .setAmount(transfer.getAmount().stripTrailingZeros().toPlainString())
                                .setFromSourceLock(transfer.getFromSourceLock() == 1)
                                .setToTargetLock(transfer.getToTargetLock() == 1)
                                .setBusinessType(transfer.getSubject())
                                .setSubBusinessType(transfer.getSubSubject())
                                .setStatus(transfer.getStatus())
                                .setSourceAccountId(transfer.getSourceAccountId())
                                .setTargetAccountId(transfer.getTargetAccountId())
                                .build())
                        .collect(Collectors.toList());
            }
            response = GetBalanceBatchTransferResponse.newBuilder()
                    .setTask(BalanceTransferTask.newBuilder()
                            .setId(transferTask.getId())
                            .setClientOrderId(transferTask.getClientOrderId())
                            .setDesc(transferTask.getDesc())
                            .setStatus(transferTask.getStatus())
                            .setCreated(transferTask.getCreated())
                            .setUpdated(transferTask.getUpdated())
                            .build())
                    .addAllTransferObj(transferObjList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBalanceBatchTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getBalanceBatchTransfer error", e);
            observer.onError(e);
            observer.onCompleted();
        }
    }

    @Override
    public void balanceBatchLockPosition(BalanceBatchLockPositionRequest request, StreamObserver<BalanceBatchOperatePositionResponse> observer) {
        BalanceBatchOperatePositionResponse response;
        try {
            List<BalanceLockPositionObj> lockPositionObjList = request.getLockPositionObjList();
            List<BalanceBatchLockPosition> balanceBatchLockPositionList = lockPositionObjList.stream()
                    .map(lockPositionObj -> BalanceBatchLockPosition.builder()
                            .userId(lockPositionObj.getUserId())
                            .accountId(0L)
                            .tokenId(lockPositionObj.getTokenId())
                            .amount(new BigDecimal(lockPositionObj.getAmount()))
                            .desc(lockPositionObj.getDesc())
                            .toPositionLocked(lockPositionObj.getToPositionLocked() ? 1 : 0)
                            .accountId(lockPositionObj.getAccountId())
                            .build())
                    .collect(Collectors.toList());
            BalanceBatchOperatePositionTask operatePositionTask = accountService.batchLockPosition(request.getHeader(), request.getClientOrderId(),
                    request.getDesc(), balanceBatchLockPositionList);
            response = BalanceBatchOperatePositionResponse.newBuilder()
                    .setTask(getBatchOperateTask(operatePositionTask))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BalanceBatchOperatePositionResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("balanceBatchLockPosition error", e);
            response = BalanceBatchOperatePositionResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_BATCH_LOCK_POSITION_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void getBalanceBatchLockPosition(GetBalanceBatchLockPositionRequest request, StreamObserver<GetBalanceBatchLockPositionResponse> observer) {
        GetBalanceBatchLockPositionResponse response;
        try {
            BalanceBatchOperatePositionResult<BalanceBatchLockPosition> operateResult = accountService.getBalanceBatchLockPositionResult(request.getHeader(), request.getClientOrderId());
            List<BalanceLockPositionObj> lockPositionObjList = operateResult.getOperateItemList().stream()
                    .map(operateItem -> BalanceLockPositionObj.newBuilder()
                            .setUserId(operateItem.getUserId())
                            .setTokenId(operateItem.getTokenId())
                            .setAmount(operateItem.getAmount().stripTrailingZeros().toPlainString())
                            .setDesc(operateItem.getDesc())
                            .setToPositionLocked(operateItem.getToPositionLocked() == 1)
                            .setLockId(operateItem.getLockId())
                            .setRemainUnlockedAmount(operateItem.getRemainUnlockedAmount().stripTrailingZeros().toPlainString())
                            .setStatus(operateItem.getStatus())
                            .setAccountId(operateItem.getAccountId())
                            .build())
                    .collect(Collectors.toList());
            response = GetBalanceBatchLockPositionResponse.newBuilder()
                    .setTask(getBatchOperateTask(operateResult.getOperateTask()))
                    .addAllLockPositionObj(lockPositionObjList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBalanceBatchLockPositionResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getBalanceBatchLockPosition error", e);
            observer.onError(e);
        }
    }

    @Override
    public void balanceBatchUnlockPosition(BalanceBatchUnlockPositionRequest request, StreamObserver<BalanceBatchOperatePositionResponse> observer) {
        BalanceBatchOperatePositionResponse response;
        try {
            List<BalanceUnlockPositionObj> unlockPositionObjList = request.getUnlockPositionObjList();
            List<BalanceBatchUnlockPosition> balanceBatchUnlockPositionList = unlockPositionObjList.stream()
                    .map(unlockPositionObj -> BalanceBatchUnlockPosition.builder()
                            .userId(unlockPositionObj.getUserId())
                            .accountId(0L)
                            .tokenId(unlockPositionObj.getTokenId())
                            .amount(new BigDecimal(unlockPositionObj.getAmount()))
                            .desc(unlockPositionObj.getDesc())
                            .fromPositionLocked(unlockPositionObj.getFromPositionLocked() ? 1 : 0)
                            .accountId(unlockPositionObj.getAccountId())
                            .build())
                    .collect(Collectors.toList());
            BalanceBatchOperatePositionTask operatePositionTask = accountService.batchUnlockPosition(request.getHeader(), request.getClientOrderId(),
                    request.getDesc(), balanceBatchUnlockPositionList);
            response = BalanceBatchOperatePositionResponse.newBuilder()
                    .setTask(getBatchOperateTask(operatePositionTask))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BalanceBatchOperatePositionResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("balanceBatchLockPosition error", e);
            response = BalanceBatchOperatePositionResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_BATCH_UNLOCK_POSITION_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void balanceLockPosition(BalanceLockPositionRequest request,
                                    StreamObserver<BalanceLockPositionResponse> observer) {
        BalanceLockPositionResponse response;
        try {
            String clientOrderId = request.getClientOrderId();
            BalanceLockPositionObj lockPositionObj = request.getLockPosition().toBuilder().setUserId(request.getHeader().getUserId()).build();
            BaseResult baseResult = accountService.balanceLockPosition(request.getHeader(), clientOrderId, lockPositionObj);
            if (baseResult.isSuccess()) {
                response = BalanceLockPositionResponse.getDefaultInstance();
            } else {
                response = BalanceLockPositionResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BalanceLockPositionResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("balanceLockPosition error", e);
            response = BalanceLockPositionResponse.newBuilder().setRet(
                    BrokerErrorCode.BALANCE_BATCH_LOCK_POSITION_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void balanceUnlockPosition(BalanceUnlockPositionRequest request,
                                      StreamObserver<BalanceUnlockPositionResponse> observer) {
        BalanceUnlockPositionResponse response;
        try {
            String clientOrderId = request.getClientOrderId();
            BalanceUnlockPositionObj unlockPositionObj = request.getUnlockPosition().toBuilder().setUserId(request.getHeader().getUserId()).build();
            BaseResult baseResult = accountService.balanceUnlockPosition(request.getHeader(), clientOrderId, unlockPositionObj);
            if (baseResult.isSuccess()) {
                response = BalanceUnlockPositionResponse.getDefaultInstance();
            } else {
                response = BalanceUnlockPositionResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BalanceUnlockPositionResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("balanceUnlockPosition error", e);
            response = BalanceUnlockPositionResponse.newBuilder().setRet(
                    BrokerErrorCode.BALANCE_BATCH_UNLOCK_POSITION_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    private io.bhex.broker.grpc.account.BalanceBatchOperatePositionTask getBatchOperateTask(BalanceBatchOperatePositionTask operatePositionTask) {
        return io.bhex.broker.grpc.account.BalanceBatchOperatePositionTask.newBuilder()
                .setId(operatePositionTask.getId())
                .setClientOrderId(operatePositionTask.getClientOrderId())
                .setType(io.bhex.broker.grpc.account.BalanceBatchOperatePositionTask.OperateType.forNumber(operatePositionTask.getType()))
                .setDesc(operatePositionTask.getDesc())
                .setStatus(operatePositionTask.getStatus())
                .setCreated(operatePositionTask.getCreated())
                .setUpdated(operatePositionTask.getUpdated())
                .build();
    }

    @Override
    public void getBalanceBatchUnLockPosition(GetBalanceBatchUnlockPositionRequest request, StreamObserver<GetBalanceBatchUnlockPositionResponse> observer) {
        GetBalanceBatchUnlockPositionResponse response;
        try {
            BalanceBatchOperatePositionResult<BalanceBatchUnlockPosition> operateResult = accountService.getBalanceBatchUnlockPositionResult(request.getHeader(), request.getClientOrderId());
            List<BalanceUnlockPositionObj> lockPositionObjList = operateResult.getOperateItemList().stream()
                    .map(operateItem -> BalanceUnlockPositionObj.newBuilder()
                            .setUserId(operateItem.getUserId())
                            .setTokenId(operateItem.getTokenId())
                            .setAmount(operateItem.getAmount().stripTrailingZeros().toPlainString())
                            .setDesc(operateItem.getDesc())
                            .setFromPositionLocked(operateItem.getFromPositionLocked() == 1)
                            .setLockId(operateItem.getLockId())
                            .setStatus(operateItem.getStatus())
                            .setAccountId(operateItem.getAccountId())
                            .build())
                    .collect(Collectors.toList());
            response = GetBalanceBatchUnlockPositionResponse.newBuilder()
                    .setTask(getBatchOperateTask(operateResult.getOperateTask()))
                    .addAllUnlockPositionObj(lockPositionObjList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBalanceBatchUnlockPositionResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getBalanceBatchUnLockPosition error", e);
            observer.onError(e);
        }
    }

    @Override
    public void balanceMapping(BalanceMappingRequest request, StreamObserver<BalanceMappingResponse> observer) {
        BalanceMappingResponse response = null;
        try {
            BaseResult baseResult = accountService.balanceMapping(request.getHeader(), request.getClientOrderId(),
                    request.getSourceUserId(), request.getSourceTokenId(), request.getSourceAmount(), request.getFromSourceLock(), request.getToTargetLock(),
                    request.getTargetUserId(), request.getTargetTokenId(), request.getTargetAmount(), request.getFromTargetLock(), request.getToSourceLock(),
                    request.getBusinessType());
            if (baseResult.isSuccess()) {
                response = BalanceMappingResponse.getDefaultInstance();
            } else {
                response = BalanceMappingResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = BalanceMappingResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" balanceMapping error", e);
            response = BalanceMappingResponse.newBuilder().setRet(BrokerErrorCode.MAPPING_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void getThirdPartyAccount(GetThirdPartyAccountRequest request, StreamObserver<GetThirdPartyAccountResponse> observer) {
        GetThirdPartyAccountResponse response = GetThirdPartyAccountResponse.getDefaultInstance();
        try {
            response = thirdPartyUserService.getVirtualUser(request.getHeader(), request.getThirdUserId(), request.getUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetThirdPartyAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getThirdPartyAccount error", e);
            response = GetThirdPartyAccountResponse.newBuilder().setRet(BrokerErrorCode.SYSTEM_ERROR.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void subAccountTransferLimit(SubAccountTransferLimitRequest request, StreamObserver<SubAccountTransferLimitResponse> observer) {
        SubAccountTransferLimitResponse response;
        try {
            accountService.subAccountTransferLimit(request.getHeader(), request.getSubAccountId(), request.getLimitTime());
            response = SubAccountTransferLimitResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SubAccountTransferLimitResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("subAccountTransferLimit error", e);
            observer.onError(e);
        }
    }

    @Override
    public void createThirdPartyAccount(CreateThirdPartyAccountRequest request, StreamObserver<CreateThirdPartyAccountResponse> observer) {
        CreateThirdPartyAccountResponse response = CreateThirdPartyAccountResponse.getDefaultInstance();
        try {
            Long userId = thirdPartyUserService.virtualUserRegister(request.getHeader(), request.getThirdUserId());
            response = CreateThirdPartyAccountResponse.newBuilder().setUserId(userId).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateThirdPartyAccountResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" createThirdPartyAccount error", e);
            response = CreateThirdPartyAccountResponse.newBuilder().setRet(BrokerErrorCode.SYSTEM_ERROR.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void createThirdPartyToken(CreateThirdPartyTokenRequest request, StreamObserver<CreateThirdPartyTokenResponse> observer) {
        CreateThirdPartyTokenResponse response = CreateThirdPartyTokenResponse.getDefaultInstance();
        try {
            response = thirdPartyUserService.virtualUserLogin(request.getHeader(), request.getThirdUserId(), request.getUserId(), request.getAccountType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CreateThirdPartyTokenResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" createThirdPartyToken error", e);
            response = CreateThirdPartyTokenResponse.newBuilder().setRet(BrokerErrorCode.SYSTEM_ERROR.code()).build();
            observer.onNext(response);
            observer.onCompleted();

        }
    }

    @Override
    public void thirdPartyUserTransferIn(ThirdPartyUserTransferInRequest request, StreamObserver<ThirdPartyUserTransferInResponse> observer) {
        ThirdPartyUserTransferInResponse response = null;
        try {
            BaseResult baseResult = accountService.thirdPartyUserTransfer(request.getHeader(), request.getClientOrderId(), true, request.getThirdUserId(), request.getUserId(),
                    AccountType.fromAccountTypeEnum(request.getAccountType()), request.getTokenId(), request.getAmount());
            log.info("thirdPartyUserTransferIn -> request:{}, result:{}", JsonUtil.defaultGson().toJson(request), JsonUtil.defaultGson().toJson(baseResult));
            if (baseResult.isSuccess()) {
                response = ThirdPartyUserTransferInResponse.getDefaultInstance();
            } else {
                response = ThirdPartyUserTransferInResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ThirdPartyUserTransferInResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" thirdPartyUserTransferIn error", e);
            response = ThirdPartyUserTransferInResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void thirdPartyUserTransferOut(ThirdPartyUserTransferOutRequest request, StreamObserver<ThirdPartyUserTransferOutResponse> observer) {
        ThirdPartyUserTransferOutResponse response = null;
        try {
            BaseResult baseResult = accountService.thirdPartyUserTransfer(request.getHeader(), request.getClientOrderId(), false, request.getThirdUserId(), request.getUserId(),
                    AccountType.fromAccountTypeEnum(request.getAccountType()), request.getTokenId(), request.getAmount());
            if (baseResult.isSuccess()) {
                response = ThirdPartyUserTransferOutResponse.getDefaultInstance();
            } else {
                response = ThirdPartyUserTransferOutResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ThirdPartyUserTransferOutResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" thirdPartyUserTransferOut error", e);
            response = ThirdPartyUserTransferOutResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void institutionalUserTransfer(InstitutionalUserTransferRequest request, StreamObserver<InstitutionalUserTransferResponse> observer) {
        InstitutionalUserTransferResponse response = null;
        try {
            BaseResult baseResult = accountService.institutionalUserTransfer(request.getHeader(), request.getClientOrderId(), request.getUserId(), request.getTokenId(), request.getAmount(),
                    request.getToLock(), request.getBusinessType(), request.getSubBusinessType());
            if (baseResult.isSuccess()) {
                response = InstitutionalUserTransferResponse.getDefaultInstance();
            } else {
                response = InstitutionalUserTransferResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = InstitutionalUserTransferResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" institutionalUserTransfer error", e);
            response = InstitutionalUserTransferResponse.newBuilder().setRet(BrokerErrorCode.BALANCE_TRANSFER_FAILED.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void thirdPartyUserBalance(ThirdPartyUserBalanceRequest request, StreamObserver<ThirdPartyUserBalanceResponse> observer) {
        ThirdPartyUserBalanceResponse response = null;
        try {
            BaseResult<List<Balance>> baseResult = accountService.getThirdPartyUserBalance(request.getHeader(), request.getThirdUserId(), request.getUserId(),
                    AccountType.fromAccountTypeEnum(request.getAccountType()));
            if (baseResult.isSuccess()) {
                response = ThirdPartyUserBalanceResponse.newBuilder().addAllBalances(baseResult.getData()).build();
            } else {
                response = ThirdPartyUserBalanceResponse.newBuilder().setRet(baseResult.getCode()).build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = ThirdPartyUserBalanceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" thirdPartyUserBalance error", e);
            response = ThirdPartyUserBalanceResponse.newBuilder().setRet(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR.code()).build();
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    @Override
    public void closeSubAccountTransferLimit(CloseSubAccountTransferLimitRequest request, StreamObserver<CloseSubAccountTransferLimitResponse> observer) {
        CloseSubAccountTransferLimitResponse response;
        try {
            accountService.closeSubAccountTransferLimit(request.getHeader(), request.getSubAccountId());
            response = CloseSubAccountTransferLimitResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = CloseSubAccountTransferLimitResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("closeSubAccountTransferLimit error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getAirDropDetail(GetAirDropDetailRequest request, StreamObserver<GetAirDropDetailResponse> observer) {
        GetAirDropDetailResponse response;
        try {
            List<BalanceBatchTransfer> transferList = accountService.getAirDropDetailResult(request.getHeader(),
                    request.getSubBusinessSubject(), request.getStatus(), request.getFromId(), request.getEndId(),
                    request.getStartTime(), request.getEndTime(),
                    request.getLimit());
            List<BalanceTransferObj> transferObjList = new ArrayList<>();
            if (!CollectionUtils.isEmpty(transferList)) {
                transferObjList = transferList.stream()
                        .map(transfer -> BalanceTransferObj.newBuilder()
                                .setSourceUserId(transfer.getSourceUserId())
                                .setTargetUserId(transfer.getTargetUserId())
                                .setTokenId(transfer.getTokenId())
                                .setAmount(transfer.getAmount().stripTrailingZeros().toPlainString())
                                .setFromSourceLock(transfer.getFromSourceLock() == 1)
                                .setToTargetLock(transfer.getToTargetLock() == 1)
                                .setBusinessType(transfer.getSubject())
                                .setSubBusinessType(transfer.getSubSubject())
                                .setStatus(transfer.getStatus())
                                .setSourceAccountId(transfer.getSourceAccountId())
                                .setTargetAccountId(transfer.getTargetAccountId())
                                .build())
                        .collect(Collectors.toList());
            }
            response = GetAirDropDetailResponse.newBuilder()
                    .addAllTransferObj(transferObjList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetAirDropDetailResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getAirDropDetail error", e);
            observer.onError(e);
            observer.onCompleted();
        }
    }

    @Override
    public void getBalanceBatchTransferDetail(GetBalanceBatchTransferDetailRequest request, StreamObserver<GetBalanceBatchTransferDetailResponse> observer) {
        GetBalanceBatchTransferDetailResponse response;
        try {
            List<BalanceBatchTransfer> transferList = accountService.getBalanceBatchTransferDetail(request.getHeader(), request.getToId(), request.getLimit());
            List<BalanceTransferObj> transferObjList = new ArrayList<>();
            if (!CollectionUtils.isEmpty(transferList)) {
                transferObjList = transferList.stream()
                        .map(transfer -> BalanceTransferObj.newBuilder()
                                .setId(transfer.getId())
                                .setSourceUserId(transfer.getSourceUserId())
                                .setTargetUserId(transfer.getTargetUserId())
                                .setTokenId(transfer.getTokenId())
                                .setAmount(transfer.getAmount().stripTrailingZeros().toPlainString())
                                .setFromSourceLock(transfer.getFromSourceLock() == 1)
                                .setToTargetLock(transfer.getToTargetLock() == 1)
                                .setBusinessType(transfer.getSubject())
                                .setSubBusinessType(transfer.getSubSubject())
                                .setStatus(transfer.getStatus())
                                .setSourceAccountId(transfer.getSourceAccountId())
                                .setTargetAccountId(transfer.getTargetAccountId())
                                .setClientOrderId(transfer.getClientOrderId())
                                .build())
                        .collect(Collectors.toList());
            }
            response = GetBalanceBatchTransferDetailResponse.newBuilder()
                    .addAllTransferObj(transferObjList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBalanceBatchTransferDetailResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getBalanceBatchTransferDetail error", e);
            observer.onError(e);
            observer.onCompleted();
        }
    }

    @Override
    public void getBalanceTransferDetail(GetBalanceTransferDetailRequest request, StreamObserver<GetBalanceTransferDetailResponse> observer) {
        GetBalanceTransferDetailResponse response;
        try {
            List<BalanceTransfer> transferList = accountService.getBalanceTransferDetail(request.getHeader(), request.getToId(), request.getLimit());
            List<BalanceTransferObj> transferObjList = new ArrayList<>();
            if (!CollectionUtils.isEmpty(transferList)) {
                transferObjList = transferList.stream()
                        .map(transfer -> BalanceTransferObj.newBuilder()
                                .setId(transfer.getId())
                                .setSourceUserId(transfer.getSourceUserId())
                                .setTargetUserId(transfer.getTargetUserId())
                                .setTokenId(transfer.getTokenId())
                                .setAmount(transfer.getAmount().stripTrailingZeros().toPlainString())
                                .setFromSourceLock(transfer.getFromSourceLock() == 1)
                                .setToTargetLock(transfer.getToTargetLock() == 1)
                                .setBusinessType(transfer.getSubject())
                                .setSubBusinessType(transfer.getSubSubject())
                                .setStatus(transfer.getStatus())
                                .setSourceAccountId(transfer.getSourceAccountId())
                                .setTargetAccountId(transfer.getTargetAccountId())
                                .setClientOrderId(transfer.getClientOrderId())
                                .build())
                        .collect(Collectors.toList());
            }
            response = GetBalanceTransferDetailResponse.newBuilder()
                    .addAllTransferObj(transferObjList)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetBalanceTransferDetailResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error(" getBalanceTransferDetail error", e);
            observer.onError(e);
            observer.onCompleted();
        }
    }
}

