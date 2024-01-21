package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.FinanceLimitType;
import io.bhex.broker.server.domain.FinanceProductType;
import io.bhex.broker.server.primary.mapper.FinanceLimitStatisticsMapper;
import io.bhex.broker.server.primary.mapper.FinanceProductMapper;
import io.bhex.broker.server.primary.mapper.FinanceWalletMapper;
import io.bhex.broker.server.model.FinanceLimitStatistics;
import io.bhex.broker.server.model.FinanceProduct;
import io.bhex.broker.server.model.FinanceRecord;
import io.bhex.broker.server.model.FinanceWallet;
import io.bhex.broker.server.util.PageUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class FinanceLimitStatisticsService {

    @Resource
    private FinanceLimitStatisticsMapper financeLimitStatisticsMapper;

    @Resource
    private FinanceWalletMapper financeWalletMapper;

    @Resource
    private FinanceProductMapper financeProductMapper;

    @Resource
    private FinanceLimitStatisticsService financeLimitStatisticsService;

    @Transactional(rollbackFor = Throwable.class)
    public void purchaseCheckFinanceLimit(FinanceProduct product, Long userId, Long accountId, BigDecimal amount) {
        String statisticsTime = new SimpleDateFormat("yyyyMMdd").format(new Date(System.currentTimeMillis()));

        // 个人额度
        FinanceLimitStatistics userLimit = financeLimitStatisticsMapper.getFinanceLimitStatisticsLock(product.getOrgId(), product.getId(), userId, FinanceLimitType.USER_LIMIT.getType(), "0");
        // 平台总限额
        FinanceLimitStatistics totalLimit = financeLimitStatisticsMapper.getFinanceLimitStatisticsLock(product.getOrgId(), product.getId(), 0L, FinanceLimitType.TOTAL_LIMIT.getType(), "0");

        // 若增加数量为正数时，则需验证用户相关限额
        BigDecimal afterUserLimitUsed = userLimit == null ? amount : userLimit.getUsed().add(amount);
        BigDecimal afterTotalLimitUsed = totalLimit == null ? amount : totalLimit.getUsed().add(amount);

        // 检查用户个人额度
        if (afterUserLimitUsed.compareTo(product.getUserLimit()) > 0) {
            log.info(" purchaseCheckFinanceLimit touch user limit: product:{} , accountId:{} , amount:{} , afterAmount:{} , statisticsTime:{}",
                    product.getId(), accountId, amount.toPlainString(), afterUserLimitUsed.toPlainString(), statisticsTime);
            throw new BrokerException(BrokerErrorCode.FINANCE_USER_LIMIT_OUT);
        }

        // 检查平台总额度
        if (afterTotalLimitUsed.compareTo(product.getTotalLimit()) > 0) {
            log.info(" purchaseCheckFinanceLimit touch total limit: product:{} , accountId:{} , amount:{} , afterAmount:{} , statisticsTime:{}",
                    product.getId(), accountId, amount.toPlainString(), afterTotalLimitUsed.toPlainString(), statisticsTime);
            throw new BrokerException(BrokerErrorCode.FINANCE_TOTAL_LIMIT_OUT);
        }

        Long currentTime = System.currentTimeMillis();
        // 修改用户限额
        if (userLimit == null) {
            userLimit = FinanceLimitStatistics.builder()
                    .orgId(product.getOrgId())
                    .userId(userId)
                    .accountId(accountId)
                    .productId(product.getId())
                    .token(product.getToken())
                    .type(FinanceLimitType.USER_LIMIT.getType())
                    .statisticsTime("0")
                    .used(amount)
                    .createdAt(currentTime)
                    .updatedAt(currentTime)
                    .build();
            financeLimitStatisticsMapper.insert(userLimit);
        } else {
            financeLimitStatisticsMapper.updateFinanceLimitStatisticsUsed(userLimit.getId(), amount, currentTime);
        }

        // 修改平台总限额
        if (totalLimit == null) {
            totalLimit = FinanceLimitStatistics.builder()
                    .orgId(product.getOrgId())
                    .userId(0L)
                    .accountId(0L)
                    .productId(product.getId())
                    .token(product.getToken())
                    .type(FinanceLimitType.TOTAL_LIMIT.getType())
                    .statisticsTime("0")
                    .used(amount)
                    .createdAt(currentTime)
                    .updatedAt(currentTime)
                    .build();
            financeLimitStatisticsMapper.insert(totalLimit);
        } else {
            financeLimitStatisticsMapper.updateFinanceLimitStatisticsUsed(totalLimit.getId(), amount, currentTime);
        }
    }

    /**
     * 申购失败或者赎回成功到账后释放理财产品限额<br>
     * 当日限额是个问题，这里先都给注释掉，以后再说
     */
    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRED)
    public void releaseFinanceLimit(FinanceRecord financeRecord) {
        Long currentTime = System.currentTimeMillis();

        // 个人额度
        FinanceLimitStatistics userLimit = financeLimitStatisticsMapper.getFinanceLimitStatisticsLock(financeRecord.getOrgId(), financeRecord.getProductId(), financeRecord.getUserId(), FinanceLimitType.USER_LIMIT.getType(), "0");
        // 平台总限额
        FinanceLimitStatistics totalLimit = financeLimitStatisticsMapper.getFinanceLimitStatisticsLock(financeRecord.getOrgId(), financeRecord.getProductId(), 0L, FinanceLimitType.TOTAL_LIMIT.getType(), "0");

        if (userLimit == null || totalLimit == null) {
            log.error(" releaseFinanceLimit lose limit record from db. userLimit:{}, totalLimit:{}",
                    JsonUtil.defaultGson().toJson(userLimit), JsonUtil.defaultGson().toJson(totalLimit));
            return;
        }
        // 若增加数量为正数时，则需验证用户相关限额
        BigDecimal afterUserLimitUsed = userLimit.getUsed().subtract(financeRecord.getAmount());
        BigDecimal afterTotalLimitUsed = totalLimit.getUsed().subtract(financeRecord.getAmount());

        // 用户个人限额
        if (afterUserLimitUsed.compareTo(BigDecimal.ZERO) >= 0) {
            financeLimitStatisticsMapper.updateFinanceLimitStatisticsUsed(userLimit.getId(), financeRecord.getAmount().negate(), currentTime);
        } else {
            log.error(" releaseFinanceLimit calculate a negative value for userLimit, please check!!! productId:{}, accountId:{}, userLimit:{}, financeRecord:{}",
                    financeRecord.getProductId(), financeRecord.getAccountId(), JsonUtil.defaultGson().toJson(userLimit), JsonUtil.defaultGson().toJson(financeRecord));
        }
        // 平台总限额
        if (afterTotalLimitUsed.compareTo(BigDecimal.ZERO) >= 0) {
            financeLimitStatisticsMapper.updateFinanceLimitStatisticsUsed(totalLimit.getId(), financeRecord.getAmount().negate(), currentTime);
        } else {
            log.error(" releaseFinanceLimit calculate a negative value for totalLimit, please check!!! productId:{}, totalLimit:{}, financeRecord:{}",
                    financeRecord.getProductId(), JsonUtil.defaultGson().toJson(totalLimit), JsonUtil.defaultGson().toJson(financeRecord));
        }
    }

    // 暂时没找到这个方法的使用意义。如有必要再调用
    public void computeFinanceTotalLimit() {
        List<FinanceProduct> productList = financeProductMapper.queryVisibleProductListByType(FinanceProductType.CURRENT.type());
        if (!CollectionUtils.isEmpty(productList)) {
            for (FinanceProduct product : productList) {
                int page = 1;
                int pageSize = 100;
                BigDecimal totalUsed = BigDecimal.ZERO;
                while (true) {
                    int startIndex = PageUtil.getStartIndex(page, pageSize);
                    List<FinanceWallet> walletList = financeWalletMapper.getFinanceWalletByOrgIdAndProductId(product.getOrgId(), product.getId(), startIndex, pageSize);
                    if (CollectionUtils.isEmpty(walletList)) {
                        break;
                    }
                    totalUsed = totalUsed.add(walletList.stream().map(record -> record.getBalance().add(record.getPurchase())).reduce(BigDecimal.ZERO, BigDecimal::add));
                    page++;
                }

                try {
                    financeLimitStatisticsService.updateFinanceTotalLimitTransaction(product, totalUsed);
                } catch (Exception e) {
                    log.error(" updateFinanceTotalLimitTransaction exception:{} amount:{}", JsonUtil.defaultGson().toJson(product), totalUsed.toPlainString(), e);
                }
            }
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    public void updateFinanceTotalLimitTransaction(FinanceProduct product, BigDecimal used) {
        // 平台总限额
        FinanceLimitStatistics limitStatistics = financeLimitStatisticsMapper.getFinanceLimitStatisticsLock(product.getOrgId(), product.getId(), 0L, FinanceLimitType.TOTAL_LIMIT.getType(), "0");
        if (limitStatistics == null) {
            return;
        }

        // 修改已使用数量
        FinanceLimitStatistics updateObj = FinanceLimitStatistics.builder()
                .id(limitStatistics.getId())
                .used(used)
                .updatedAt(System.currentTimeMillis())
                .build();
        financeLimitStatisticsMapper.updateByPrimaryKeySelective(updateObj);
    }

}
