package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.finance.RedeemFinanceResponse;
import io.bhex.broker.server.domain.*;
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
public class FinanceRedeemService {

    @Resource
    private AccountService accountService;

    @Resource
    private FinanceProductService financeProductService;

    @Resource
    private FinanceTransferService financeTransferService;

    @Resource
    private FinanceLimitStatisticsService financeLimitStatisticsService;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private FinanceRecordMapper financeRecordMapper;

    @Resource
    private FinanceWalletMapper financeWalletMapper;

    @Resource
    private FinanceWalletChangeFlowMapper financeWalletChangeFlowMapper;

    @Transactional(rollbackFor = Throwable.class)
    public RedeemFinanceResponse redeem(Header header, Long orgId, Long userId, Long productId, BigDecimal amount) {

        FinanceProduct product = financeProductService.getFinanceProductCacheById(productId);
        if (product == null || product.getStatus() == 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST);
        }
        if (product.getStartTime() > System.currentTimeMillis()) {
            throw new BrokerException(BrokerErrorCode.FINANCE_NOT_OPEN);
        }
        if (product.getAllowRedeem() == 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PRODUCT_STOP_REDEEM);
        }
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        /*
         * 赎回的步骤
         * 1、校验可赎回金额
         * 2、优先扣除待计息数据
         * 3、如果不够，则扣除计息中的资产
         */
        // 1、
        Long accountId = accountService.getAccountId(orgId, userId);
        FinanceWallet wallet = financeWalletMapper.getFinanceWalletLock(orgId, userId, productId);
        if (wallet == null) {
            throw new BrokerException(BrokerErrorCode.FINANCE_REDEEM_INSUFFICIENT_BALANCE);
        }
        BigDecimal availableRedeem = wallet.getBalance().add(wallet.getPurchase());
        if (amount.compareTo(availableRedeem) > 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_REDEEM_INSUFFICIENT_BALANCE);
        }

        if (amount.compareTo(product.getTradeScale()) < 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_REDEEM_AMOUNT_TOO_SMALL);
        }
        if (amount.subtract(product.getTradeScale()).remainder(product.getTradeScale()).compareTo(BigDecimal.ZERO) != 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_ILLEGAL_REDEEM_AMOUNT);
        }

        // 2、3、
        Long currentTime = System.currentTimeMillis();
        BigDecimal purchaseUse = BigDecimal.ZERO;
        BigDecimal balanceUse = BigDecimal.ZERO;
        if (wallet.getPurchase().compareTo(BigDecimal.ZERO) > 0) {
            if (wallet.getPurchase().compareTo(amount) >= 0) {
                purchaseUse = amount;
                balanceUse = BigDecimal.ZERO;
            } else {
                purchaseUse = wallet.getPurchase();
                balanceUse = amount.subtract(wallet.getPurchase());
            }
        } else {
            balanceUse = amount;
        }

        // 大额的检测，看用户赎回金额的大小来决定是否给实时到账，确定赎回记录的类型和状态
        FinanceRedeemType redeemType = FinanceRedeemType.NORMAL;
        FinanceRedeemStatus redeemStatus = FinanceRedeemStatus.WAITING;
        if (amount.compareTo(product.getRedeemLimitAmount()) >= 0) {
            // 赎回金额大于 赎回限制金额，则认为是大额单
            redeemType = FinanceRedeemType.BIG;
            redeemStatus = FinanceRedeemStatus.BIG_WAITING;
            // TODO give a warning
        }

        // 插入记录
        FinanceRecord redeemRecord = FinanceRecord.builder()
                .orgId(product.getOrgId())
                .userId(userId)
                .accountId(accountId)
                .transferId(sequenceGenerator.getLong())
                .productId(productId)
                .type(FinanceRecordType.REDEEM.type())
                .token(product.getToken())
                .amount(amount)
                .status(redeemStatus.getStatus())
                .redeemType(redeemType.getType())
                .platform(header.getPlatform().name())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .createdAt(currentTime)
                .updatedAt(currentTime)
                .build();
        financeRecordMapper.insertSelective(redeemRecord);

        BigDecimal originalBalance = wallet.getBalance(), originalPurchase = wallet.getPurchase();
        String originalWallet = JsonUtil.defaultGson().toJson(wallet);
        if (purchaseUse.compareTo(BigDecimal.ZERO) > 0) {
            wallet.setPurchase(wallet.getPurchase().subtract(purchaseUse));
        }
        if (balanceUse.compareTo(BigDecimal.ZERO) > 0) {
            wallet.setBalance(wallet.getBalance().subtract(balanceUse));
        }
        BigDecimal changedBalance = wallet.getBalance(), changedPurchase = wallet.getPurchase();
        String changedWallet = JsonUtil.defaultGson().toJson(wallet);

        FinanceWalletChangeFlow walletChangeFlow = FinanceWalletChangeFlow.builder()
                .walletId(wallet.getId())
                .orgId(product.getOrgId())
                .userId(userId)
                .accountId(accountId)
                .productId(productId)
                .token(product.getToken())
                .changeType(FinanceWalletChangeType.REDEEM.type())
                .amount(amount)
                .referenceId(redeemRecord.getId())
                .originalBalance(originalBalance)
                .changedBalance(changedBalance)
                .originalPurchase(originalPurchase)
                .changedPurchase(changedPurchase)
                .originalWallet(originalWallet)
                .changedWallet(changedWallet)
                .created(currentTime)
                .build();
        financeWalletChangeFlowMapper.insertSelective(walletChangeFlow);
        financeWalletMapper.updateWalletAfterRedeem(wallet.getId(), balanceUse.negate(), purchaseUse.negate(), amount, currentTime);
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
                redeemExecute(redeemRecord);
            } catch (Exception e) {
                // ignore
            }
        });
        return RedeemFinanceResponse.newBuilder().setRecordId(redeemRecord.getId()).build();
    }

    @Transactional(rollbackFor = Throwable.class)
    public void redeemExecute(FinanceRecord record) throws Exception {
        Long currentTime = System.currentTimeMillis();

        // 锁定赎回记录
        FinanceRecord redeemRecord = financeRecordMapper.getFinanceRecordLock(record.getId());
        if (redeemRecord == null) {
            log.error(" redeemHandle cannot find record, recordId:{}", record.getId());
            throw new Exception("handle redeem failed, record not found");
        }

        if (redeemRecord.getType() != FinanceRecordType.REDEEM.type()) {
            log.error(" redeemHandle find record type is not redeem, recordId:{}", record.getId());
            throw new Exception(" handle redeem failed, record type is not redeem");
        }

        // 如果遇到 BIG_WAITING，需要确认并手工处理
        if (redeemRecord.getStatus() == FinanceRedeemStatus.BIG_WAITING.getStatus()) {
            log.error(" redeemHandle find record status is BIG_WAITING, please handle!!!");
            return;
        }
        if (redeemRecord.getStatus() == FinanceRedeemStatus.FINISHED.getStatus()) {
            return;
        }
        if (redeemRecord.getStatus() != FinanceRedeemStatus.WAITING.getStatus()
                && redeemRecord.getStatus() != FinanceRedeemStatus.EXECUTE_EXCEPTION.getStatus()) {
            log.error(" redeemHandle find record status is not WAITING or EXECUTE_EXCEPTION, recordId:{}", record.getId());
            throw new Exception("handle redeem failed, record status is not WAITING or EXECUTE_EXCEPTION");
        }

        //TODO 操作平台划转
        FinanceTransferResult transferResult = financeTransferService.redeemTransfer(redeemRecord);
        log.info(" redeem record:[{}] transfer status is {}", JsonUtil.defaultGson().toJson(redeemRecord), transferResult);
        if (transferResult == FinanceTransferResult.SUCCESS) {
            // 重置限额数据
            financeLimitStatisticsService.releaseFinanceLimit(redeemRecord);
            // wallet redeem=redeem-record.getAmount
            FinanceWallet wallet = financeWalletMapper.getFinanceWalletLock(redeemRecord.getOrgId(), redeemRecord.getUserId(), redeemRecord.getProductId());
            financeWalletMapper.updateWalletAfterRedeemSuccess(wallet.getId(), redeemRecord.getAmount().negate(), currentTime);

            // 修改赎回状态
            FinanceRecord updateObj = FinanceRecord.builder()
                    .id(redeemRecord.getId())
                    .status(FinanceRedeemStatus.FINISHED.getStatus())
                    .updatedAt(currentTime)
                    .build();
            financeRecordMapper.updateByPrimaryKeySelective(updateObj);
            log.info(" redeemHandle success, record:[{}]", JsonUtil.defaultGson().toJson(redeemRecord));
        } else {
            log.error(" redeemHandle get {} transfer status, record:[{}]", transferResult, JsonUtil.defaultGson().toJson(record));
        }
    }

}
