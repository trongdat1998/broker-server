package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.FXRate;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.finance.GetFinanceWalletListResponse;
import io.bhex.broker.grpc.finance.GetSingleFinanceWalletResponse;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.PageUtil;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FinanceWalletService {

    private static final BigDecimal DAY_OF_YEAR = new BigDecimal(365);

    @Resource
    private BasicService basicService;

    @Resource
    private FinanceProductService financeProductService;

    @Resource
    private FinanceLimitStatisticsMapper financeLimitStatisticsMapper;

    @Resource
    private FinanceWalletMapper financeWalletMapper;

    @Resource
    private FinanceRecordMapper financeRecordMapper;

    @Resource
    private FinanceProductMapper financeProductMapper;

    @Resource
    private FinanceInterestMapper financeInterestMapper;

    @Resource
    private FinanceInterestDataMapper financeInterestDataMapper;

    @Resource
    private FinanceWalletChangeFlowMapper financeWalletChangeFlowMapper;

    @Resource
    private FinanceTaskMapper financeTaskMapper;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    public GetFinanceWalletListResponse getFinanceWalletList(Long orgId, Long userId) {
        List<FinanceWallet> walletList = queryWalletBalance(orgId, userId);
        walletList = walletList.stream().filter(wallet -> wallet.getBalance().add(wallet.getPurchase().add(wallet.getRedeem())).compareTo(BigDecimal.ZERO) > 0).collect(Collectors.toList());
        BigDecimal btcValue = walletList.stream().map(FinanceWallet::getBtcValue).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal usdtValue = walletList.stream().map(FinanceWallet::getUsdtValue).reduce(BigDecimal.ZERO, BigDecimal::add);
        if (!CollectionUtils.isEmpty(walletList)) {
            return GetFinanceWalletListResponse.newBuilder()
                    .addAllWallets(walletList.stream().map(this::convertWallet).collect(Collectors.toList()))
                    .setBtcValue(btcValue.stripTrailingZeros().toPlainString())
                    .setUsdtValue(usdtValue.stripTrailingZeros().toPlainString())
                    .build();
        }
        return GetFinanceWalletListResponse.newBuilder().setBtcValue("0").setUsdtValue("0").build();
    }

    public List<FinanceWallet> queryWalletBalance(Long orgId, Long userId) {
        List<FinanceWallet> walletList = financeWalletMapper.getFinanceWalletByOrgIdAndUserId(orgId, userId);
        for (FinanceWallet wallet : walletList) {
            setValuationForWallet(wallet);
        }
        return walletList;
    }

    public GetSingleFinanceWalletResponse getSingleFinanceWallet(Long orgId, Long userId, Long productId) {
        FinanceProduct product = financeProductService.getFinanceProductCacheById(productId);
        if (product == null) {
            return GetSingleFinanceWalletResponse.newBuilder().setRet(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST.code()).build();
        }
        FinanceWallet wallet = financeWalletMapper.getFinanceWallet(orgId, userId, productId);
        if (wallet == null) {
            return GetSingleFinanceWalletResponse.getDefaultInstance();
        }
        setValuationForWallet(wallet);

        // 获取用户限额
        FinanceLimitStatistics userLimit = financeLimitStatisticsMapper.getFinanceLimitStatistics(
                product.getOrgId(), productId, userId, FinanceLimitType.USER_LIMIT.getType(), "0");

        BigDecimal used = (userLimit == null || userLimit.getUsed() == null) ? BigDecimal.ZERO : userLimit.getUsed();
        BigDecimal lastLimit = product.getUserLimit().subtract(used);

        return GetSingleFinanceWalletResponse.newBuilder()
                .setWallet(this.convertWallet(wallet))
                .setUserLastLimit(lastLimit.toPlainString())
                .build();
    }

    private void setValuationForWallet(FinanceWallet wallet) {
        Rate fxRate = basicService.getV3Rate(wallet.getOrgId(), wallet.getToken());
        BigDecimal total = wallet.getBalance().add(wallet.getPurchase()).add(wallet.getRedeem());
        if (fxRate == null) {
            wallet.setBtcValue(BigDecimal.ZERO);
            wallet.setUsdtValue(BigDecimal.ZERO);
        } else {
            wallet.setBtcValue(total.multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))));
            wallet.setUsdtValue(total.multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("USDT"))));
        }
    }

    private io.bhex.broker.grpc.finance.FinanceWallet convertWallet(FinanceWallet wallet) {
        return io.bhex.broker.grpc.finance.FinanceWallet.newBuilder()
                .setId(wallet.getId())
                .setOrgId(wallet.getOrgId())
                .setUserId(wallet.getUserId())
                .setAccountId(wallet.getAccountId())
                .setProductId(wallet.getProductId())
                .setToken(wallet.getToken())
                .setBalance(wallet.getBalance().stripTrailingZeros().toPlainString())
                .setPurchase(wallet.getPurchase().stripTrailingZeros().toPlainString())
                .setRedeem(wallet.getRedeem().stripTrailingZeros().toPlainString())
                .setLastProfit(wallet.getLastProfit().stripTrailingZeros().toPlainString())
                .setTotalProfit(wallet.getTotalProfit().stripTrailingZeros().toPlainString())
                .setBtcValue(wallet.getBtcValue().stripTrailingZeros().toPlainString())
                .setUsdtValue(wallet.getUsdtValue().stripTrailingZeros().toPlainString())
                .build();
    }

    /**
     * 每日计息任务<br>
     * 1.根据用户前一日的lastBalanceChangeFlow计算昨日应该利息<br>
     * 2.如果用户当天有申购和赎回记录，
     */
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void financeInterestTask() throws Exception {
        long todayStartMilliseconds = new DateTime().withTime(0, 0, 0, 0).getMillis();
        DateTime lastDayDate = new DateTime().minusDays(1);
        String statisticsTime = new SimpleDateFormat("yyyyMMdd").format(lastDayDate.toDate());
        long lastDayStartTime = lastDayDate.withTime(0, 0, 0, 0).getMillis();
        long lastDayEndTime = todayStartMilliseconds;
        // lock finance_wallet table
        long currentTime = System.currentTimeMillis();
        int recordSize = financeWalletMapper.lockWalletTable();
        if (recordSize <= 0) {
            log.warn(" financeInterestTask preCheck, wallet data is null, check!!!");
            return;
        }

        List<FinanceProduct> productList = financeProductMapper.queryVisibleProductListByType(FinanceProductType.CURRENT.type());
        for (FinanceProduct product : productList) {
            if (product.getStatus() == 0) {
                continue;
            }
            // check task if hash been executed
            FinanceTask lastFinanceTask = financeTaskMapper.selectOne(FinanceTask.builder().orgId(product.getOrgId()).productId(product.getId()).statisticsTime(statisticsTime).build());
            if (lastFinanceTask != null) {
                log.error(" financeInterestTask has been executed product:[id:{}, statisticsTime:{}]", product.getId(), statisticsTime);
                continue;
            }
            // find product interestRate config
            FinanceInterest interest =
                    financeInterestMapper.getFinanceInterestByOrgIdAndProductIdAndStatisticsTime(product.getOrgId(), product.getId(), statisticsTime);
            if (interest == null) {
                interest = FinanceInterest.builder()
                        .orgId(product.getOrgId())
                        .productId(product.getId())
                        .token(product.getToken())
                        .statisticsTime(statisticsTime)
                        .rate(new BigDecimal("0.05"))
                        .status(0)
                        .createdAt(currentTime)
                        .updatedAt(currentTime)
                        .build();
                financeInterestMapper.insertSelective(interest);
            }
            BigDecimal originalRate = interest.getRate();
            BigDecimal rate = interest.getRate().divide(DAY_OF_YEAR, 8, RoundingMode.DOWN);

            int productWalletSize = 0;
            BigDecimal totalBalance = BigDecimal.ZERO, totalInterest = BigDecimal.ZERO;
            int page = 1, pageSize = 100;
            while (true) {
                int startIndex = PageUtil.getStartIndex(page++, pageSize);
                List<FinanceWallet> walletList = financeWalletMapper.getFinanceWalletByPage(product.getId(), startIndex, pageSize);
                if (CollectionUtils.isEmpty(walletList)) {
                    break;
                }
                productWalletSize += walletList.size();

                List<FinanceInterestData> interestDataList = Lists.newArrayList();
                List<FinanceRecord> interestRecordList = Lists.newArrayList();

                /*
                 * 1、找到wallet对应的昨天的最后一条流水, 如果找不到前一日的流水，跳过。
                 * 2、校验一下流水(不过也没啥必要，因为流水都是按照要求去写的)
                 * 3、根据最后一条流水记录，计算昨日应发利息并生成相应的利息发放log和资产记录
                 * 4、如有必要，更新当前wallet中的balance和purchase
                 */
                for (FinanceWallet wallet : walletList) {
                    // 1、
                    FinanceWalletChangeFlow lastWalletChangeFlow = financeWalletChangeFlowMapper.findLastRecordLastDay(wallet.getId(), lastDayStartTime, lastDayEndTime);
                    if (lastWalletChangeFlow == null) {
                        log.warn(" financeInterestTask cannot find most recent wallet flow before {}, wallet: {}",
                                todayStartMilliseconds, JsonUtil.defaultGson().toJson(wallet));
                        continue;
                    }
                    // 2、
                    BigDecimal lastBalance = lastWalletChangeFlow.getChangedBalance();
                    BigDecimal lastPurchase = lastWalletChangeFlow.getChangedPurchase();
                    FinanceWallet lastWallet = JsonUtil.defaultGson().fromJson(lastWalletChangeFlow.getChangedWallet(), FinanceWallet.class);
                    if (lastWallet == null || (lastBalance.compareTo(lastWallet.getBalance()) != 0)
                            || (lastPurchase.compareTo(lastWallet.getPurchase()) != 0)) {
                        log.error(" financeInterestTask find wallet flow is wrong, flow: {}", JsonUtil.defaultGson().toJson(lastWalletChangeFlow));
                        throw new Exception(" financeInterestTask find wallet flow is wrong");
                    }
                    // 3、
                    log.info("     |---- financeInterestTask forEach calculate - currentWalletInfo:{}", JsonUtil.defaultGson().toJson(wallet));
                    log.info("     |---- financeInterestTask forEach calculate - lastWalletInfo:{}", lastWalletChangeFlow.getChangedWallet());
                    BigDecimal lastProfit = BigDecimal.ZERO;
                    if (lastWallet.getBalance().compareTo(BigDecimal.ZERO) > 0) {
                        FinanceInterestData interestData = FinanceInterestData.builder()
                                .orgId(wallet.getOrgId())
                                .userId(wallet.getUserId())
                                .accountId(wallet.getAccountId())
                                .productId(wallet.getProductId())
                                .token(wallet.getToken())
                                .balance(lastBalance)
                                .purchase(lastPurchase)
                                .originalRate(originalRate)
                                .rate(rate)
                                .statisticsTime(statisticsTime)
                                .createdAt(currentTime)
                                .updatedAt(currentTime)
                                .build();
                        interestDataList.add(interestData);

                        FinanceRecord record = createInterestRecord(product, interestData, statisticsTime);
                        interestRecordList.add(record);
                        log.info("     |---- financeInterestTask forEach calculate - needHandle balance value, interest:{} = {} * {}",
                                record.getAmount().toPlainString(), lastBalance.toPlainString(), rate.toPlainString());

                        lastProfit = record.getAmount();
                        totalBalance = totalBalance.add(lastBalance);
                        totalInterest = totalInterest.add(record.getAmount());
                    }
                    financeWalletMapper.updateLastProfit(wallet.getId(), lastProfit, currentTime);

                    /*
                     * 4、修正wallet中的balance和purchase
                     * 4.1 如果lastFlow中的计息和待计息都为0，并且当前资产中的计息和待计息的和也为0，标示今天没有新的申购和赎回，不用去写(没任何变动的)流水，也不用去重新修正wallet中的balance和purchase
                     * 4.2 如果purchase > 0，那么
                     *     如果不存在申购和赎回
                     *         balance = lastBalance + lastPurchase;
                     *         purchase = 0;
                     *     如果存在申购和赎回，则根据申购和赎回的顺序，恢复一下wallet中的balance和purchase的值
                     *
                     */
                    if (lastBalance.compareTo(BigDecimal.ZERO) <= 0 && lastPurchase.compareTo(BigDecimal.ZERO) <= 0
                            && wallet.getBalance().add(wallet.getPurchase()).compareTo(BigDecimal.ZERO) < 0) {
                        log.info(" |---- financeInterestTask forEach calculate - lastTotalAsset and currentTotalAsset are all zero. continue;");
                        continue;
                    }
                    lastWallet.setBalance(lastWallet.getBalance().add(lastWallet.getPurchase()));
                    lastWallet.setPurchase(BigDecimal.ZERO);
                    log.info("     |---- financeInterestTask forEach calculate - lastPurchase > 0, set balance={}, purchase = {}",
                            lastWallet.getBalance().toPlainString(), lastWallet.getPurchase().toPlainString());

                    BigDecimal originalBalance = wallet.getBalance(), changedBalance = originalBalance;
                    BigDecimal originalPurchase = wallet.getPurchase(), changedPurchase = originalPurchase;
                    String originalWallet = JsonUtil.defaultGson().toJson(wallet), changedWallet = originalWallet;

                    // 找到今日的申购和赎回记录
                    List<FinanceWalletChangeFlow> walletChangeFlows = financeWalletChangeFlowMapper.queryWalletFlowBeforeTime(wallet.getId(), todayStartMilliseconds).stream()
                            .filter(flow -> flow.getChangeType() == FinanceWalletChangeType.PURCHASE.type()
                                    || flow.getChangeType() == FinanceWalletChangeType.REDEEM.type()).collect(Collectors.toList());
                    if (!CollectionUtils.isEmpty(walletChangeFlows)) {
                        for (FinanceWalletChangeFlow walletChangeFlow : walletChangeFlows) {
                            if (walletChangeFlow.getChangeType() == FinanceWalletChangeType.PURCHASE.type()) {
                                lastWallet.setPurchase(lastWallet.getPurchase().add(walletChangeFlow.getAmount()));
                                log.info("     |---- financeInterestTask forEach calculate - purchase record, set balance={}, purchase = {}",
                                        lastWallet.getBalance().toPlainString(), lastWallet.getPurchase().toPlainString());
                            } else if (walletChangeFlow.getChangeType() == FinanceWalletChangeType.REDEEM.type()) {
                                BigDecimal purchaseUse = BigDecimal.ZERO;
                                BigDecimal balanceUse = BigDecimal.ZERO;
                                if (lastWallet.getPurchase().compareTo(BigDecimal.ZERO) > 0) {
                                    if (lastWallet.getPurchase().compareTo(walletChangeFlow.getAmount()) >= 0) {
                                        purchaseUse = walletChangeFlow.getAmount();
                                        balanceUse = BigDecimal.ZERO;
                                    } else {
                                        purchaseUse = lastWallet.getPurchase();
                                        balanceUse = walletChangeFlow.getAmount().subtract(lastWallet.getPurchase());
                                    }
                                } else {
                                    balanceUse = walletChangeFlow.getAmount();
                                }
                                lastWallet.setBalance(lastWallet.getBalance().subtract(balanceUse));
                                lastWallet.setPurchase(lastWallet.getPurchase().subtract(purchaseUse));
                                log.info("     |---- financeInterestTask forEach calculate - redeem record, set balance={}, purchase = {}",
                                        lastWallet.getBalance().toPlainString(), lastWallet.getPurchase().toPlainString());
                            }
                        }
                    }
                    if (wallet.getBalance().add(wallet.getPurchase()).compareTo(lastWallet.getBalance().add(lastWallet.getPurchase())) != 0) {
                        log.error(" financeInterestTask resetBalanceAndPurchase get a different sum value for oldWalletData and newWalletData. resetWallet:{}, currentWallet:{}",
                                JsonUtil.defaultGson().toJson(lastWallet), JsonUtil.defaultGson().toJson(wallet));
                        throw new Exception(" financeInterestTask resetBalanceAndPurchase get a different sum value for oldWalletData and newWalletData. walletId:" + wallet.getId());
                    }

                    changedBalance = lastWallet.getBalance();
                    changedPurchase = lastWallet.getPurchase();
                    wallet.setBalance(lastWallet.getBalance());
                    wallet.setPurchase(lastWallet.getPurchase());
                    changedWallet = JsonUtil.defaultGson().toJson(wallet);
                    financeWalletMapper.updateWalletAfterDailyTaskManualExecute(wallet.getId(), wallet.getBalance(), wallet.getPurchase(), currentTime);

                    FinanceWalletChangeFlow walletChangeFlow = FinanceWalletChangeFlow.builder()
                            .walletId(wallet.getId())
                            .orgId(wallet.getOrgId())
                            .userId(wallet.getUserId())
                            .accountId(wallet.getAccountId())
                            .productId(wallet.getProductId())
                            .token(wallet.getToken())
                            .changeType(FinanceWalletChangeType.INTEREST.type())
                            .amount(BigDecimal.ZERO)
                            .referenceId(sequenceGenerator.getLong() * -1)
                            .originalBalance(originalBalance)
                            .changedBalance(changedBalance)
                            .originalPurchase(originalPurchase)
                            .changedPurchase(changedPurchase)
                            .originalWallet(originalWallet)
                            .changedWallet(changedWallet)
                            .created(currentTime)
                            .build();
                    financeWalletChangeFlowMapper.insertSelective(walletChangeFlow);
                }
                if (!CollectionUtils.isEmpty(interestDataList)) {
                    financeInterestDataMapper.insertList(interestDataList);
                }
                if (!CollectionUtils.isEmpty(interestRecordList)) {
                    financeRecordMapper.insertList(interestRecordList);
                }
            }
            FinanceTask financeTask = FinanceTask.builder()
                    .orgId(product.getOrgId())
                    .productId(product.getId())
                    .token(product.getToken())
                    .statisticsTime(statisticsTime)
                    .totalBalance(totalBalance)
                    .totalInterest(totalInterest)
                    .created(currentTime)
                    .build();
            financeTaskMapper.insertSelective(financeTask);
            log.info(" financeInterestTask forEach calculate - productId:{}, productWalletSize:{}, totalBalance:{}, totalInterest:{}",
                    product.getId(), productWalletSize, totalBalance.toPlainString(), totalInterest.toPlainString());
        }
        log.info(" financeInterestTask execute success ::::::::statisticsTime:{} cost:{} ",
                statisticsTime, (System.currentTimeMillis() - currentTime));
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public boolean manualFinanceInterestTask(String statisticsTime) throws Exception {
        // lock finance_wallet table
        long currentTime = System.currentTimeMillis();
        int recordSize = financeWalletMapper.lockWalletTable();
        if (recordSize <= 0) {
            log.error(" manualFinanceInterestTask preCheck, wallet data is null, check!!!");
            return true;
        }

        log.info(" manualFinanceInterestTask execute start..., statisticsTime:{}, walletDateSize:{}, startTime:{}", statisticsTime, recordSize, currentTime);
        try {
            List<FinanceProduct> productList = financeProductMapper.queryVisibleProductListByType(FinanceProductType.CURRENT.type());
            for (FinanceProduct product : productList) {
                if (product.getStatus() == 0) {
                    continue;
                }
                // check if task has been executed
                FinanceTask lastFinanceTask = financeTaskMapper.selectOne(FinanceTask.builder().orgId(product.getOrgId()).productId(product.getId()).statisticsTime(statisticsTime).build());
                if (lastFinanceTask != null) {
                    log.error(" manualFinanceInterestTask has been executed product:[id:{}, statisticsTime:{}]", product.getId(), statisticsTime);
                    continue;
                }
                // find product interestRate config
                FinanceInterest interest =
                        financeInterestMapper.getFinanceInterestByOrgIdAndProductIdAndStatisticsTime(product.getOrgId(), product.getId(), statisticsTime);
                if (interest == null) {
                    interest = FinanceInterest.builder()
                            .orgId(product.getOrgId())
                            .productId(product.getId())
                            .token(product.getToken())
                            .statisticsTime(statisticsTime)
                            .rate(new BigDecimal("0.05"))
                            .status(0)
                            .createdAt(currentTime)
                            .updatedAt(currentTime)
                            .build();
                    financeInterestMapper.insertSelective(interest);
                }
                BigDecimal originalRate = interest.getRate();
                BigDecimal rate = interest.getRate().divide(DAY_OF_YEAR, 8, RoundingMode.DOWN);

                log.info(" manualFinanceInterestTask forEach calculate - productId:{}", product.getId());
                /*
                 * 按照用户Finance的资产数据，从tb_finance_interest中取出这一天执行的利息发放利率，
                 */
                int productWalletSize = 0;
                BigDecimal totalBalance = BigDecimal.ZERO, totalInterest = BigDecimal.ZERO;
                int page = 1, pageSize = 100;
                while (true) {
                    int startIndex = PageUtil.getStartIndex(page++, pageSize);
                    List<FinanceWallet> walletList = financeWalletMapper.getFinanceWalletByPage(product.getId(), startIndex, pageSize);
                    if (CollectionUtils.isEmpty(walletList)) {
                        break;
                    }
                    productWalletSize += walletList.size();

                    List<FinanceInterestData> interestDataList = Lists.newArrayList();
                    List<FinanceRecord> interestRecordList = Lists.newArrayList();

                    /*
                     * 1、如果账号上已经完全没有钱了，标记昨日收益=0，continue
                     * 2、处理计息，更新昨日收益和总计息
                     * 3、处理待计息。写入(空)流水
                     */
                    for (FinanceWallet wallet : walletList) {
                        log.info("     |---- manualFinanceInterestTask forEach calculate - originalWalletInfo:{}", JsonUtil.defaultGson().toJson(wallet));
                        // 1、
                        if (wallet.getBalance().compareTo(BigDecimal.ZERO) <= 0 && wallet.getPurchase().compareTo(BigDecimal.ZERO) <= 0) {
                            if (wallet.getLastProfit().compareTo(BigDecimal.ZERO) > 0) {
                                financeWalletMapper.updateLastProfit(wallet.getId(), BigDecimal.ZERO, currentTime);
                            }
                            continue;
                        }
                        // 2、
                        BigDecimal lastProfit = BigDecimal.ZERO;
                        if (wallet.getBalance().compareTo(BigDecimal.ZERO) > 0) {
                            FinanceInterestData interestData = createFinanceInterestData(wallet, originalRate, rate, statisticsTime);
                            interestDataList.add(interestData);

                            // create interest record to finance_record
                            FinanceRecord record = createInterestRecord(product, interestData, statisticsTime);
                            interestRecordList.add(record);
                            log.info("     |---- manualFinanceInterestTask forEach calculate - needHandle balance value, interest:{} = {} * {}",
                                    record.getAmount().toPlainString(), wallet.getBalance().toPlainString(), rate.toPlainString());

                            lastProfit = record.getAmount();
                            totalBalance = totalBalance.add(wallet.getBalance());
                            totalInterest = totalInterest.add(record.getAmount());
                        }
                        // 更新昨日收益和总收益
                        financeWalletMapper.updateLastProfit(wallet.getId(), lastProfit, currentTime);

                        // 3、处理待计息。写入一条变动=purchase(了能为0)的流水，保证在有资产的情况下每天都有一条流水
                        if (wallet.getPurchase().compareTo(BigDecimal.ZERO) > 0) {
                            log.info("     |---- manualFinanceInterestTask forEach calculate - needHandle purchase value, balance={} + {}, purchase = {} + {}",
                                    wallet.getBalance().toPlainString(), wallet.getPurchase().toPlainString(),
                                    wallet.getPurchase().toPlainString(), wallet.getPurchase().negate().toPlainString());
                            financeWalletMapper.updateWalletWhenDailyTaskAutoExecute(wallet.getId(), wallet.getPurchase(), wallet.getPurchase().negate(), currentTime);
                        }

                        BigDecimal originalBalance = wallet.getBalance(), changedBalance = wallet.getBalance().add(wallet.getPurchase());
                        BigDecimal originalPurchase = wallet.getPurchase(), changedPurchase = BigDecimal.ZERO;
                        String originalWallet = JsonUtil.defaultGson().toJson(wallet);

                        wallet.setBalance(wallet.getBalance().add(wallet.getPurchase()));
                        wallet.setPurchase(BigDecimal.ZERO);
                        String changedWallet = JsonUtil.defaultGson().toJson(wallet);

                        FinanceWalletChangeFlow walletChangeFlow = FinanceWalletChangeFlow.builder()
                                .walletId(wallet.getId())
                                .orgId(wallet.getOrgId())
                                .userId(wallet.getUserId())
                                .accountId(wallet.getAccountId())
                                .productId(wallet.getProductId())
                                .token(wallet.getToken())
                                .changeType(FinanceWalletChangeType.INTEREST.type())
                                .amount(wallet.getPurchase())
                                .referenceId(sequenceGenerator.getLong() * -1)
                                .originalBalance(originalBalance)
                                .changedBalance(changedBalance)
                                .originalPurchase(originalPurchase)
                                .changedPurchase(changedPurchase)
                                .originalWallet(originalWallet)
                                .changedWallet(changedWallet)
                                .created(currentTime)
                                .build();
                        financeWalletChangeFlowMapper.insertSelective(walletChangeFlow);
                    }
                    if (!CollectionUtils.isEmpty(interestDataList)) {
                        financeInterestDataMapper.insertList(interestDataList);
                    }
                    if (!CollectionUtils.isEmpty(interestRecordList)) {
                        financeRecordMapper.insertList(interestRecordList);
                    }
                }
                FinanceTask financeTask = FinanceTask.builder()
                        .orgId(product.getOrgId())
                        .productId(product.getId())
                        .token(product.getToken())
                        .statisticsTime(statisticsTime)
                        .totalBalance(totalBalance)
                        .totalInterest(totalInterest)
                        .created(currentTime)
                        .build();
                financeTaskMapper.insertSelective(financeTask);
                log.info(" manualFinanceInterestTask forEach calculate - productId:{}, productWalletSize:{}, totalBalance:{}, totalInterest:{}",
                        product.getId(), productWalletSize, totalBalance.toPlainString(), totalInterest.toPlainString());
            }
            log.info(" manualFinanceInterestTask execute success ::::::::statisticsTime:{} cost:{} ",
                    statisticsTime, (System.currentTimeMillis() - currentTime));
            return true;
        } catch (Exception e) {
            log.error(" manualFinanceInterestTask execute has exception", e);
            throw new Exception(" manualFinanceInterestTask catch exception", e);
        }
    }

    private FinanceInterestData createFinanceInterestData(FinanceWallet wallet, BigDecimal originalRate, BigDecimal rate, String statisticsTime) {
        long currentTime = System.currentTimeMillis();
        FinanceInterestData interestData = FinanceInterestData.builder()
                .orgId(wallet.getOrgId())
                .userId(wallet.getUserId())
                .accountId(wallet.getAccountId())
                .productId(wallet.getProductId())
                .token(wallet.getToken())
                .balance(wallet.getBalance())
                .purchase(wallet.getPurchase())
                .originalRate(originalRate)
                .rate(rate)
                .statisticsTime(statisticsTime)
                .createdAt(currentTime)
                .updatedAt(currentTime)
                .build();
        log.info("     |---- manualFinanceInterestTask forEach calculate - getInterestData:{}", JsonUtil.defaultGson().toJson(interestData));
        return interestData;
    }

    private FinanceRecord createInterestRecord(FinanceProduct product, FinanceInterestData interestData, String statisticsTime) {
        long currentTime = System.currentTimeMillis();
        BigDecimal amount = interestData.getBalance().multiply(interestData.getOriginalRate())
                .divide(DAY_OF_YEAR, product.getInterestScale().stripTrailingZeros().scale(), RoundingMode.DOWN);
        FinanceRecord interestRecord = FinanceRecord.builder()
                .orgId(interestData.getOrgId())
                .userId(interestData.getUserId())
                .accountId(interestData.getAccountId())
                .transferId(sequenceGenerator.getLong())
                .productId(interestData.getProductId())
                .type(FinanceRecordType.INTEREST.type())
                .token(interestData.getToken())
                .amount(amount)
                .status(FinanceInterestStatus.UN_GRANT.status())
                .redeemType(FinanceRedeemType.NORMAL.getType()) // 这里设置这个值没什么用，但是不设置insert会报错
                .statisticsTime(statisticsTime)
                .createdAt(currentTime)
                .updatedAt(currentTime)
                .build();
        if (amount.compareTo(product.getInterestMinAmount()) < 0) {
            log.info("         manualFinanceInterestTask - calculateInterest is less than product interestMinAmount limit, set amount = 0!! interestData:{}", JsonUtil.defaultGson().toJson(interestData));
            interestRecord.setAmount(BigDecimal.ZERO);
            interestRecord.setStatus(FinanceInterestStatus.FINISHED.status());
        }
        log.info("     |---- manualFinanceInterestTask forEach calculate - getInterestRecord:{}, interestDate:{}",
                JsonUtil.defaultGson().toJson(interestRecord), JsonUtil.defaultGson().toJson(interestData));
        return interestRecord;
    }

}
