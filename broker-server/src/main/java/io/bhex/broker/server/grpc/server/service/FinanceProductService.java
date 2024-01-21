package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.FinanceLimitType;
import io.bhex.broker.server.domain.FinanceProductType;
import io.bhex.broker.server.primary.mapper.FinanceLimitStatisticsMapper;
import io.bhex.broker.server.primary.mapper.FinanceProductMapper;
import io.bhex.broker.server.model.FinanceLimitStatistics;
import io.bhex.broker.server.model.FinanceProduct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FinanceProductService {

    private static final String FINANCE_PRODUCT_LIST_CACHE_KEY = "finance_product_list:";
    private static final String FINANCE_PRODUCT_CACHE_KEY = "finance_product:";

    @Resource
    private FinanceProductMapper financeProductMapper;

    @Resource
    private FinanceLimitStatisticsMapper financeLimitStatisticsMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    public List<FinanceProduct> getFinanceProductByOrgId(Long orgId) {
        List<FinanceProduct> allList = this.getFinanceProductListCache(FinanceProductType.CURRENT.type());
        if (CollectionUtils.isEmpty(allList)) {
            return Lists.newArrayList();
        }

        List<FinanceProduct> productList = Lists.newArrayList();
        for (FinanceProduct product : allList) {
            if (!product.getOrgId().equals(orgId)) {
                continue;
            }
            productList.add(product);
        }
        return productList;
    }

    public FinanceProduct getFinanceProductCacheById(Long id) {
        String cacheKey = FINANCE_PRODUCT_CACHE_KEY + id;
        String cacheString = redisTemplate.opsForValue().get(cacheKey);
        if (StringUtils.isNotBlank(cacheString)) {
            return JsonUtil.defaultGson().fromJson(cacheString, FinanceProduct.class);
        }

        FinanceProduct product = financeProductMapper.selectByPrimaryKey(id);
        if (product == null || product.getStatus() == 0) {
            throw new BrokerException(BrokerErrorCode.FINANCE_PRODUCT_NOT_EXIST);
        }
        redisTemplate.opsForValue().set(cacheKey, JsonUtil.defaultGson().toJson(product), 5, TimeUnit.MINUTES);
        return product;
    }

    public List<FinanceProduct> getFinanceProductListCache(int type) {
        String cacheKey = FINANCE_PRODUCT_LIST_CACHE_KEY + type;
        String cacheString = redisTemplate.opsForValue().get(cacheKey);
        if (!Strings.isNullOrEmpty(cacheString)) {
            return JsonUtil.defaultGson().fromJson(cacheString, new TypeToken<List<FinanceProduct>>() {
            }.getType());
        }

        List<FinanceProduct> productList = financeProductMapper.queryVisibleProductListByType(type);
        redisTemplate.opsForValue().set(cacheKey, JsonUtil.defaultGson().toJson(productList), 5, TimeUnit.MINUTES);
        for (FinanceProduct product : productList) {
            redisTemplate.opsForValue().set(FINANCE_PRODUCT_CACHE_KEY + product.getId(),
                    JsonUtil.defaultGson().toJson(product), 5, TimeUnit.MINUTES);
        }
        return productList;
    }

    public void resetFinanceProductCache(int type) {
        FinanceProduct queryObject = FinanceProduct.builder()
                .type(type)
                .build();
        List<FinanceProduct> productList = financeProductMapper.select(queryObject);
        // 清除 list 缓存
        String listCacheKey = FINANCE_PRODUCT_LIST_CACHE_KEY + type;
        redisTemplate.delete(listCacheKey);

        List<String> singleProductCacheKeyList = productList.stream()
                .map(product -> FINANCE_PRODUCT_CACHE_KEY + product.getId())
                .collect(Collectors.toList());
        redisTemplate.delete(singleProductCacheKeyList);
    }

    public List<io.bhex.broker.grpc.finance.FinanceProduct> packageProductList(List<FinanceProduct> productList) {
        if (CollectionUtils.isEmpty(productList)) {
            return Lists.newArrayList();
        }

        String statisticsTime = new SimpleDateFormat("yyyyMMdd").format(new Date());
        List<io.bhex.broker.grpc.finance.FinanceProduct> resultList = Lists.newArrayList();
        for (FinanceProduct product : productList) {
            io.bhex.broker.grpc.finance.FinanceProduct productResult = this.convertProduct(product);
            // 获取平台总限额使用情况
            FinanceLimitStatistics totalLimit = financeLimitStatisticsMapper.getFinanceLimitStatistics(
                    product.getOrgId(), product.getId(), 0L, FinanceLimitType.TOTAL_LIMIT.getType(), "0");

            BigDecimal totalUsed = (totalLimit == null || totalLimit.getUsed() == null) ? BigDecimal.ZERO : totalLimit.getUsed();
            // 计算剩余每日限额
            BigDecimal totalLastLimit = product.getTotalLimit().subtract(totalUsed);

            // 设置剩余每日限额
            productResult = productResult.toBuilder()
                    .setDailyLastLimit(totalLastLimit.toPlainString())
                    .setTotalLastLimit(totalLastLimit.toPlainString())
                    .build();
            resultList.add(productResult);
        }
        return resultList;
    }

    public io.bhex.broker.grpc.finance.FinanceProduct convertProduct(FinanceProduct product) {
        return io.bhex.broker.grpc.finance.FinanceProduct.newBuilder()
                .setId(product.getId())
                .setOrgId(product.getOrgId())
                .setToken(product.getToken())
                .setType(product.getType())
                .setTotalLimit(product.getTotalLimit().stripTrailingZeros().toPlainString())
                .setDailyLimit(product.getDailyLimit().stripTrailingZeros().toPlainString())
                .setUserLimit(product.getUserLimit().stripTrailingZeros().toPlainString())
                .setTradeScale(product.getTradeScale().stripTrailingZeros().toPlainString())
                .setTradeMinAmount(product.getTradeMinAmount().stripTrailingZeros().toPlainString())
                .setInterestScale(product.getInterestScale().stripTrailingZeros().toPlainString())
                .setInterestMinAmount(product.getInterestMinAmount().stripTrailingZeros().toPlainString())
                .setSevenYearRate(product.getSevenYearRate().stripTrailingZeros().toPlainString())
                .setStartTime(product.getStartTime())
                .setSort(product.getSort())
                .setShowInHomePage(product.getShowInHomePage() == 1)
                .setStatus(product.getStatus())
                .setPurchaseMinAmount(product.getTradeMinAmount().stripTrailingZeros().toPlainString())
                .setRedeemMinAmount(product.getTradeScale().stripTrailingZeros().toPlainString())
                .setAllowPurchase(product.getAllowPurchase() == 1)
                .setAllowRedeem(product.getAllowRedeem() == 1)
                .build();
    }

}
