package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.finance.PurchaseFinanceResponse;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.grpc.server.service.aspect.UserActionLogAnnotation;
import io.bhex.broker.server.grpc.server.service.aspect.SiteFunctionLimitSwitchAnnotation;
import io.bhex.broker.server.primary.mapper.FinanceProductMapper;
import io.bhex.broker.server.primary.mapper.FinanceRecordMapper;
import io.bhex.broker.server.primary.mapper.FinanceWalletChangeFlowMapper;
import io.bhex.broker.server.primary.mapper.FinanceWalletMapper;
import io.bhex.broker.server.model.FinanceProduct;
import io.bhex.broker.server.model.FinanceRecord;
import io.bhex.broker.server.model.FinanceWallet;
import io.bhex.broker.server.model.FinanceWalletChangeFlow;
import io.bhex.broker.server.util.GrpcHeaderUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class FinancePurchaseService {

    @Resource
    private AccountService accountService;

    @Resource
    private FinanceProductMapper financeProductMapper;

    @Resource
    private FinanceTransferService financeTransferService;

    @Resource
    private FinanceLimitStatisticsService financeLimitStatisticsService;

    @Resource
    private FinanceWalletChangeFlowMapper financeWalletChangeFlowMapper;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private FinanceRecordMapper financeRecordMapper;

    @Resource
    private FinanceWalletMapper financeWalletMapper;
    @Resource
    private BaseBizConfigService baseBizConfigService;

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_CREATE_PURCHASE_FINANCE,
            action = "productId:{#productId} amount:{#amount}")
    @SiteFunctionLimitSwitchAnnotation(userSwitchGroupKey = BaseConfigConstants.FROZEN_USER_BONUS_TRADE_GROUP)
    public PurchaseFinanceResponse purchase(Header header, Long orgId, Long userId, Long productId, BigDecimal amount) {
        FinanceProduct product = financeProductMapper.selectByPrimaryKey(productId);
        if (product == null || product.getStatus() == 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST);
        }
        if (product.getStartTime() > System.currentTimeMillis()) {
            throw new BrokerException(BrokerErrorCode.FINANCE_NOT_OPEN);
        }
        if (product.getAllowPurchase() == 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PRODUCT_STOP_PURCHASE);
        }
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }

        /*
         * 申购的步骤
         * 1、校验申购金额是否大于可用资产
         * 2、检查是否满足限购
         * 3、生成一笔状态为处理中申购记录
         * 4、发起转账请求
         */
        // 1、
        Long accountId = accountService.getAccountId(orgId, userId);
        Balance balance = accountService.queryTokenBalance(orgId, accountId, product.getToken());
        if (balance == null || amount.compareTo(new BigDecimal(balance.getFree())) > 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PURCHASE_INSUFFICIENT_BALANCE);
        }
        if (amount.compareTo(product.getTradeMinAmount()) < 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PURCHASE_AMOUNT_TOO_SMALL);
        }
        if (amount.subtract(product.getTradeMinAmount()).remainder(product.getTradeScale()).compareTo(BigDecimal.ZERO) != 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_ILLEGAL_PURCHASE_AMOUNT);
        }
        // 2、
        try {
            financeLimitStatisticsService.purchaseCheckFinanceLimit(product, userId, accountId, amount);
        } catch (BrokerException e) {
            return PurchaseFinanceResponse.newBuilder().setRet(e.code()).build();
        } catch (Exception e) {
            log.error("purchase catch exception: orgId:{} userId:{}, productId:{}, amount:{} ",
                    orgId, userId, productId, amount.toPlainString());
            return PurchaseFinanceResponse.newBuilder().setRet(BrokerErrorCode.FINANCE_PURCHASE_ERROR.code()).build();
        }

        // 3、
        Long currentTime = System.currentTimeMillis();
        FinanceRecord purchaseRecord = FinanceRecord.builder()
                .orgId(product.getOrgId())
                .userId(userId)
                .accountId(accountId)
                .transferId(sequenceGenerator.getLong())
                .productId(productId)
                .type(FinanceRecordType.PURCHASE.type())
                .token(product.getToken())
                .amount(amount)
                .status(FinancePurchaseStatus.WAITING.getStatus())
                .platform(header.getPlatform().name())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .createdAt(currentTime)
                .updatedAt(currentTime)
                .build();
        financeRecordMapper.insertSelective(purchaseRecord);
        // 4、
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
                purchaseExecute(purchaseRecord);
            } catch (Exception e) {
                // ignore
            }
        });
        return PurchaseFinanceResponse.newBuilder().setRecordId(purchaseRecord.getId()).build();
    }

    @Transactional(rollbackFor = Throwable.class)
    public void purchaseExecute(FinanceRecord record) throws Exception {
        // 锁定用户订单
        FinanceRecord purchaseRecord = financeRecordMapper.getFinanceRecordLock(record.getId());
        if (purchaseRecord == null) {
            log.error(" purchaseHandle cannot find record, recordId:{}", record.getId());
            throw new Exception("handle purchase failed, record not found");
        }

        if (purchaseRecord.getType() != FinanceRecordType.PURCHASE.type()) {
            log.error(" purchaseHandle find record type is not purchase, recordId:{}", record.getId());
            throw new Exception("handle purchase failed, record type is not purchase");
        }

        if (purchaseRecord.getStatus() != FinancePurchaseStatus.WAITING.getStatus()
                && purchaseRecord.getStatus() != FinancePurchaseStatus.EXECUTE_EXCEPTION.getStatus()) {
            log.error(" purchaseHandle find record status is not WAITING or EXECUTE_EXCEPTION, recordId:{}", record.getId());
            throw new Exception("handle purchase failed, record status is not WAITING or EXECUTE_EXCEPTION");
        }

        Long currentTime = System.currentTimeMillis();
        // 执行调用平台转账操作，若不成功，则减掉相关限额数量
        FinanceTransferResult transferResult = financeTransferService.purchaseTransfer(purchaseRecord);
        log.info(" purchase record:[{}] transfer status is {}", JsonUtil.defaultGson().toJson(purchaseRecord), transferResult);
        if (transferResult.equals(FinanceTransferResult.SUCCESS)) {
            FinanceWallet wallet = financeWalletMapper.getFinanceWalletLock(purchaseRecord.getOrgId(), purchaseRecord.getUserId(), purchaseRecord.getProductId());
            BigDecimal originalBalance = BigDecimal.ZERO, changedBalance = BigDecimal.ZERO;
            BigDecimal originalPurchase = BigDecimal.ZERO, changedPurchase = BigDecimal.ZERO;
            String originalWallet = "", changedWallet = "";
            if (wallet == null) {
                wallet = FinanceWallet.builder()
                        .orgId(purchaseRecord.getOrgId())
                        .userId(purchaseRecord.getUserId())
                        .accountId(purchaseRecord.getAccountId())
                        .productId(purchaseRecord.getProductId())
                        .token(purchaseRecord.getToken())
                        .balance(BigDecimal.ZERO)
                        .purchase(purchaseRecord.getAmount())
                        .redeem(BigDecimal.ZERO)
                        .lastProfit(BigDecimal.ZERO)
                        .totalProfit(BigDecimal.ZERO)
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build();
                financeWalletMapper.insert(wallet);
                changedPurchase = wallet.getPurchase();
            } else {
                originalBalance = wallet.getBalance();
                originalPurchase = wallet.getPurchase();
                originalWallet = JsonUtil.defaultGson().toJson(wallet);
                financeWalletMapper.updateWalletAfterPurchase(wallet.getId(), purchaseRecord.getAmount(), currentTime);
                wallet.setPurchase(wallet.getPurchase().add(purchaseRecord.getAmount()));
                changedBalance = wallet.getBalance();
                changedPurchase = wallet.getPurchase();
            }
            changedWallet = JsonUtil.defaultGson().toJson(wallet);

            FinanceWalletChangeFlow walletChangeFlow = FinanceWalletChangeFlow.builder()
                    .walletId(wallet.getId())
                    .orgId(purchaseRecord.getOrgId())
                    .userId(purchaseRecord.getUserId())
                    .accountId(purchaseRecord.getAccountId())
                    .productId(purchaseRecord.getProductId())
                    .token(purchaseRecord.getToken())
                    .changeType(FinanceWalletChangeType.PURCHASE.type())
                    .amount(purchaseRecord.getAmount())
                    .referenceId(purchaseRecord.getId())
                    .originalBalance(originalBalance)
                    .changedBalance(changedBalance)
                    .originalPurchase(originalPurchase)
                    .changedPurchase(changedPurchase)
                    .originalWallet(originalWallet)
                    .changedWallet(changedWallet)
                    .created(currentTime)
                    .build();
            financeWalletChangeFlowMapper.insertSelective(walletChangeFlow);

            FinanceRecord updateObj = FinanceRecord.builder()
                    .id(purchaseRecord.getId())
                    .status(FinancePurchaseStatus.FINISHED.getStatus())
                    .updatedAt(currentTime)
                    .build();
            financeRecordMapper.updateByPrimaryKeySelective(updateObj);
            log.info(" purchaseHandle success, record:[{}]", JsonUtil.defaultGson().toJson(purchaseRecord));
        } else if (transferResult.equals(FinanceTransferResult.FROM_BALANCE_NOT_ENOUGH)) {
            log.warn(" purchaseHandle get {} transfer status, recordStatus will be set to FAILED. record: {}", transferResult, JsonUtil.defaultGson().toJson(purchaseRecord));
            // 释放限额
            financeLimitStatisticsService.releaseFinanceLimit(purchaseRecord);

            // 更改record状态
            FinanceRecord updateObj = FinanceRecord.builder()
                    .id(purchaseRecord.getId())
                    .status(FinancePurchaseStatus.FAILED.getStatus())
                    .updatedAt(System.currentTimeMillis())
                    .build();
            financeRecordMapper.updateByPrimaryKeySelective(updateObj);
        } else {
            log.error(" purchaseHandle get {} transfer status, record: {}", transferResult, JsonUtil.defaultGson().toJson(purchaseRecord));
        }
    }

}
