package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.FinanceInterestStatus;
import io.bhex.broker.server.domain.FinanceProductType;
import io.bhex.broker.server.domain.FinanceTransferResult;
import io.bhex.broker.server.primary.mapper.FinanceInterestMapper;
import io.bhex.broker.server.primary.mapper.FinanceProductMapper;
import io.bhex.broker.server.primary.mapper.FinanceRecordMapper;
import io.bhex.broker.server.model.FinanceInterest;
import io.bhex.broker.server.model.FinanceProduct;
import io.bhex.broker.server.model.FinanceRecord;
import io.bhex.broker.server.util.PageUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class FinanceInterestService {

    private static final String INTEREST_TIME_CACHE_KEY = "finance:interest_time_key_%s_%s_%s";

    private static final int DAY_OF_SEVEN = 7;

    @Resource
    private FinanceProductService financeProductService;

    @Resource
    private FinanceTransferService financeTransferService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private FinanceInterestMapper financeInterestMapper;

    @Resource
    private FinanceProductMapper financeProductMapper;

    @Resource
    private FinanceRecordMapper financeRecordMapper;

    public FinanceInterest getFinanceInterestCache(Long orgId, Long productId, String statisticsTime) {
        String cacheKey = String.format(INTEREST_TIME_CACHE_KEY, orgId, productId, statisticsTime);
        String interestCache = redisTemplate.opsForValue().get(cacheKey);
        if (!Strings.isNullOrEmpty(interestCache)) {
            return JsonUtil.defaultGson().fromJson(interestCache, FinanceInterest.class);
        }

        FinanceInterest interest = financeInterestMapper.getFinanceInterestByOrgIdAndProductIdAndStatisticsTime(orgId, productId, statisticsTime);
        if (interest == null) {
            Long currentTime = System.currentTimeMillis();
            FinanceProduct financeProduct = financeProductMapper.selectByPrimaryKey(productId);
            interest = FinanceInterest.builder()
                    .orgId(orgId)
                    .productId(productId)
                    .token(financeProduct.getToken())
                    .statisticsTime(statisticsTime)
                    .rate(new BigDecimal("0.05"))
                    .status(0)
                    .createdAt(currentTime)
                    .updatedAt(currentTime)
                    .build();
            financeInterestMapper.insertSelective(interest);
        }
        redisTemplate.opsForValue().set(cacheKey, JsonUtil.defaultGson().toJson(interest), 5, TimeUnit.MINUTES);
        return interest;
    }

    public void computeFinanceProductSevenYearRate(String statisticsTime) {
        try {
            log.info(" computeFinanceProductSevenYearRate start :::::::time:{} ", statisticsTime);
            Long currentTime = System.currentTimeMillis();
            int count = 0;

            List<FinanceProduct> productList = financeProductMapper.queryVisibleProductListByType(FinanceProductType.CURRENT.type());
            for (FinanceProduct product : productList) {
                List<FinanceInterest> interestList = financeInterestMapper.getSevenFinanceInterest(product.getOrgId(), product.getId(), statisticsTime, DAY_OF_SEVEN);
                BigDecimal sevenYearRate;
                if (CollectionUtils.isEmpty(interestList)) {
                    sevenYearRate = BigDecimal.ZERO;
                } else {
                    // 七日年化 = 七日汇率加和 / 7天 （若不足7天，就是汇率的加和 / 天数）
                    // 汇率保留小数4位
                    BigDecimal totalRate = interestList.stream().map(FinanceInterest::getRate).reduce(BigDecimal.ZERO, BigDecimal::add);
                    sevenYearRate = totalRate.divide(new BigDecimal(interestList.size()), 4, RoundingMode.DOWN);
                }
                product.setSevenYearRate(sevenYearRate);
                product.setUpdatedAt(currentTime);
                financeProductMapper.updateByPrimaryKeySelective(product);
                log.info(" computeFinanceProductSevenYearRate :: time:{} productId:{} token:{} new SevenYearRate:{} ",
                        statisticsTime, product.getId(), product.getToken(), product.getSevenYearRate());
                count++;
            }

            // 清除缓存
            financeProductService.resetFinanceProductCache(FinanceProductType.CURRENT.type());
            log.info(" computeFinanceProductSevenYearRate  finished ::::::: time:{} cost:{} count:{} ",
                    statisticsTime, (System.currentTimeMillis() - currentTime), count);
        } catch (Exception e) {
            log.error(" computeFinanceProductSevenYearRate exception:{} ", statisticsTime, e);
        }
    }

    public void grantFinanceInterest(String statisticsTime, boolean checkAutoTransfer) {
        Long currentTime = System.currentTimeMillis();
        log.info(" grantFinanceInterest start ::::::: statisticsTime:{}", statisticsTime);

        List<FinanceProduct> productList = financeProductMapper.queryVisibleProductListByType(FinanceProductType.CURRENT.type());
        for (FinanceProduct product : productList) {
            if (product.getStatus() == 0) {
                continue;
            }
            if (checkAutoTransfer && product.getAutoTransfer() == 0) {
                log.error(" grantFinanceInterest of product:{} need to be manually executed", product.getId());
                continue;
            }
            int page = 1, pageSize = 100;
            while (true) {
                int startIndex = PageUtil.getStartIndex(page++, pageSize);
                List<FinanceRecord> unGrantInterestRecord = financeRecordMapper.queryUnGrantFinanceInterestRecordsByStatisticsTime(product.getId(), statisticsTime, startIndex, pageSize);
                if (CollectionUtils.isEmpty(unGrantInterestRecord)) {
                    break;
                }
                for (FinanceRecord record : unGrantInterestRecord) {
                    if (record.getStatus() == FinanceInterestStatus.FINISHED.status()) {
                        continue;
                    }
                    // 若转账数量 <= 0 则直接修改为成功状态即可，不需要转账了
                    if (record.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
                        log.warn(" grantFinanceInterest find a negative interest record:{}, please check", JsonUtil.defaultGson().toJson(record));
                        continue;
                    }
                    try {
                        FinanceTransferResult transferResult = financeTransferService.interestTransfer(record);
                        if (transferResult.equals(FinanceTransferResult.SUCCESS)) {
                            // 划转成功，修改订单状态
                            try {
                                this.updateFinanceInterestRecordToSuccess(record, currentTime);
                            } catch (Exception e) {
                                log.error(" grantFinanceInterest transfer success, but has exception when update record status, record:{}", JsonUtil.defaultGson().toJson(record), e);
                            }
                        } else {
                            log.error(" grantFinanceInterest fail: transferResult:{}, record:{}", transferResult, JsonUtil.defaultGson().toJson(record));
                        }
                    } catch (Exception e) {
                        log.error(" grantFinanceInterest catchException, record:{}", JsonUtil.defaultGson().toJson(record), e);
                    }
                }
            }
        }
        log.info(" grantFinanceInterest success ::::::: statisticsTime:{}", statisticsTime);
    }

    @Transactional(rollbackFor = Throwable.class)
    public int updateFinanceInterestRecordToSuccess(FinanceRecord record, Long currentTime) {
        FinanceRecord interestRecord = financeRecordMapper.getFinanceRecordLock(record.getId());
        if (interestRecord.getStatus() == FinanceInterestStatus.FINISHED.status()) {
            return 0;
        }

        // 修改订单状态
        FinanceRecord updateObj = FinanceRecord.builder()
                .id(interestRecord.getId())
                .status(FinanceInterestStatus.FINISHED.status())
                .updatedAt(currentTime)
                .build();
        return financeRecordMapper.updateByPrimaryKeySelective(updateObj);
    }

}
