package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.broker.server.model.FinanceInterest;
import io.bhex.broker.server.model.FinanceProduct;
import io.bhex.broker.server.model.FinanceRecord;
import io.bhex.broker.server.model.FinanceWallet;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * 同步原币多多数据到staking
 *
 * @author
 * @date
 */
@Service
@Slf4j
public class StakingSyncDataService {

    @Resource
    private FinanceRecordMapper financeRecordMapper;

    @Resource
    private FinanceProductMapper financeProductMapper;

    @Resource
    private FinanceWalletMapper financeWalletMapper;

    @Resource
    private StakingProductRebateMapper stakingProductRebateMapper;

    @Resource
    private StakingProductRebateBatchMapper stakingProductRebateBatchMapper;

    @Resource
    private StakingProductOrderBatchMapper stakingProductOrderBatchMapper;

    @Resource
    private StakingAssetBatchMapper stakingAssetBatchMapper;

    @Resource
    private StakingAssetSnapshotBatchMapper stakingAssetSnapshotBatchMapper;

    @Resource
    private StakingProductJourBatchMapper stakingProductJourBatchMapper;

    /**
     * 同步原币多多数据
     *
     * @param fromId
     * @param toId
     */
    @Transactional(rollbackFor = Throwable.class)
    public void syncFinanceData(Long orgId, Long fromId, Long toId) {
        return;
        /*if (orgId == null) {
            orgId = 6002L;
        }

        try {

            FinanceProduct financeProduct = financeProductMapper.selectByPrimaryKey(fromId);
            if (financeProduct == null) {
                log.info("finance product is null:{}", fromId);
                return;
            }

            *//*-----------------------------------------1. staking rebate--------------------------------------------------*//*
            StakingProductRebate productRebateExists = stakingProductRebateMapper.selectOne(StakingProductRebate.builder()
                    .orgId(orgId)
                    .productId(toId)
                    .status(0)
                    .build());
            if (productRebateExists != null) {
                log.info("product rebate is exists:{}", productRebateExists.toString());
                return;
            }
            List<StakingProductRebate> listStakingProductRebate = new ArrayList<>();
            DateTime stDate = new DateTime(financeProduct.getStartTime()).withTime(0, 0, 0, 0);
            DateTime endDate = stDate.plusYears(5);
            long todayMilliseconds = new DateTime().withTime(0, 0, 0, 0).getMillis();
            int i = 1;
            log.info("===staking rebate sync start===");
            while (stDate.isBefore(endDate)) {
                listStakingProductRebate.add(StakingProductRebate.builder()
                        .orgId(orgId)
                        .productId(toId)
                        .productType(1)
                        .tokenId(financeProduct.getToken())
                        .rebateDate(stDate.getMillis())
                        .type(0)
                        .rebateCalcWay(0)
                        .rebateRate(new BigDecimal("0.05"))
                        .rebateAmount(BigDecimal.ZERO)
                        .numberOfPeriods(i)
                        .status(stDate.getMillis() <= todayMilliseconds ? 2 : 0)
                        .createdAt(System.currentTimeMillis())
                        .updatedAt(System.currentTimeMillis())
                        .build());
                stDate = stDate.plusDays(1);
                i++;
            }
            listStakingProductRebate.add(StakingProductRebate.builder()
                    .orgId(orgId)
                    .productId(toId)
                    .productType(1)
                    .tokenId(financeProduct.getToken())
                    .rebateDate(endDate.getMillis())
                    .type(1)
                    .rebateCalcWay(0)
                    .rebateRate(new BigDecimal("1"))
                    .rebateAmount(BigDecimal.ZERO)
                    .numberOfPeriods(i)
                    .status(0)
                    .createdAt(System.currentTimeMillis())
                    .updatedAt(System.currentTimeMillis())
                    .build());
            stakingProductRebateBatchMapper.insertList(listStakingProductRebate);
            log.info("===staking rebate sync done===");
            *//*-----------------------------------2. staking asset and snapshotAsset---------------------------------------*//*
            Example walletExample = new Example(FinanceWallet.class);
            Example.Criteria walletCriteria = walletExample.createCriteria();
            walletCriteria.andEqualTo("orgId", orgId);
            walletCriteria.andEqualTo("productId", fromId);
            walletExample.orderBy("id");

            List<FinanceWallet> financeWalletList = financeWalletMapper.selectByExample(walletExample);
            List<StakingAsset> stakingAssetList = new ArrayList<>();
            List<StakingAssetSnapshot> stakingAssetSnapshotList = new ArrayList<>();

            StakingProductRebate stakingProductRebate = stakingProductRebateMapper.selectOne(StakingProductRebate.builder()
                    .orgId(orgId)
                    .rebateDate(todayMilliseconds)
                    .productId(toId)
                    .build());
            log.info("===finance wallet sync start===");
            for (FinanceWallet wallet : financeWalletList) {
                // asset
                stakingAssetList.add(StakingAsset.builder()
                        .orgId(orgId)
                        .productId(toId)
                        .userId(wallet.getUserId())
                        .accountId(wallet.getAccountId())
                        .productType(1)
                        .tokenId(financeProduct.getToken())
                        .totalAmount(wallet.getBalance().add(wallet.getPurchase()))
                        .currentAmount(wallet.getBalance().add(wallet.getPurchase()))
                        .lastProfit(wallet.getLastProfit())
                        .totalProfit(wallet.getTotalProfit())
                        .createdAt(System.currentTimeMillis())
                        .updatedAt(System.currentTimeMillis())
                        .build());

                // asset snapshot
                stakingAssetSnapshotList.add(StakingAssetSnapshot.builder()
                        .orgId(orgId)
                        .userId(wallet.getUserId())
                        .productId(toId)
                        .tokenId(financeProduct.getToken())
                        .dailyDate(stakingProductRebate == null ? todayMilliseconds : stakingProductRebate.getRebateDate())
                        .netAsset(wallet.getBalance())
                        .payInterest(wallet.getLastProfit())
                        .apr(new BigDecimal("0.05"))
                        .status(0)
                        .createdAt(System.currentTimeMillis())
                        .updatedAt(System.currentTimeMillis())
                        .productRebateId(stakingProductRebate == null ? 0L : stakingProductRebate.getId())
                        .build());
            }

            if (!CollectionUtils.isEmpty(stakingAssetList)) {
                stakingAssetBatchMapper.insertList(stakingAssetList);
            }

            if (!CollectionUtils.isEmpty(stakingAssetSnapshotList)) {
                stakingAssetSnapshotBatchMapper.insertList(stakingAssetSnapshotList);
            }
            log.info("===finance wallet sync done===");
            *//*-----------------------------------3. record to staking orders and jour-------------------------------------*//*
            long beforeId = 0;
            log.info("===finance record sync start===");
            while (true) {
                List<FinanceRecord> financeRecordList = financeRecordMapper.getFinanceRecordListTemp(orgId, fromId, beforeId, 1000);
                if (CollectionUtils.isEmpty(financeRecordList)) {
                    break;
                }
                List<StakingProductOrder> stakingProductOrderList = new ArrayList<>();
                List<StakingProductJour> stakingProductJourList = new ArrayList<>();

                for (FinanceRecord financeRecord : financeRecordList) {
                    // JOUR
                    stakingProductJourList.add(StakingProductJour.builder()
                            .orgId(orgId)
                            .userId(financeRecord.getUserId())
                            .accountId(financeRecord.getAccountId())
                            .productId(toId)
                            .productType(1)
                            .type(financeRecord.getType())
                            .amount(financeRecord.getAmount().abs())
                            .tokenId(financeProduct.getToken())
                            .transferId(financeRecord.getTransferId())
                            .createdAt(financeRecord.getCreatedAt())
                            .updatedAt(financeRecord.getCreatedAt())
                            .build());

                    // if purchase and redeem the insert order 0=purchase 1=redeem 2=interest
                    if (financeRecord.getType() == 0 || financeRecord.getType() == 1) {
                        int payLots = financeRecord.getAmount().divide(financeProduct.getTradeScale(), 0, RoundingMode.DOWN).intValue();
                        stakingProductOrderList.add(StakingProductOrder.builder()
                                .orgId(orgId)
                                .productId(toId)
                                .productType(1)
                                .userId(financeRecord.getUserId())
                                .accountId(financeRecord.getAccountId())
                                .transferId(financeRecord.getTransferId())
                                .payLots(payLots)
                                .payAmount(financeRecord.getType() == 0 ? financeRecord.getAmount() : BigDecimal.ZERO.subtract(financeRecord.getAmount()))
                                .orderType(financeRecord.getType())
                                .tokenId(financeProduct.getToken())
                                .takeEffectDate(new DateTime(financeRecord.getCreatedAt()).withTime(0, 0, 0, 0).getMillis())
                                .redemptionDate(0L)
                                .lastInterestDate(todayMilliseconds)
                                .canAutoRenew(0)
                                .status(1)
                                .createdAt(financeRecord.getCreatedAt())
                                .updatedAt(financeRecord.getUpdatedAt())
                                .build());
                    }
                }
                if (!CollectionUtils.isEmpty(stakingProductOrderList)) {
                    stakingProductOrderBatchMapper.insertList(stakingProductOrderList);
                }
                if (!CollectionUtils.isEmpty(stakingProductJourList)) {
                    stakingProductJourBatchMapper.insertList(stakingProductJourList);
                }
                log.info("===finance record sync done===");
                beforeId = financeRecordList.stream().mapToLong(FinanceRecord::getId).max().orElse(0L);
            }
        }
        catch (Exception ex){
            log.error("ex:{}",ex.toString());
            throw  ex;
        }*/
    }
}
