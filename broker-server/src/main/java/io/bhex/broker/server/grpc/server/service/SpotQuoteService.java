package io.bhex.broker.server.grpc.server.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.bhex.base.quote.GetRealtimeReply;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.server.grpc.client.service.GrpcQuoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author wangshouchao
 */
@Slf4j
@Service
public class SpotQuoteService {

    @Resource
    private GrpcQuoteService grpcQuoteService;

    /**
     * key: exchangeId + "_" + symbolId
     * value: price
     */
    private LoadingCache<SimpleSymbol, BigDecimal> quotePriceCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.SECONDS)
            .build(new CacheLoader<>() {
                @Override
                public BigDecimal load(SimpleSymbol key) {
                    return getCurrentPrice(key.symbolId, key.exchangeId, key.orgId);
                }
            });

    /**
     * 短暂的缓存(注意orgId不是必要的参数)
     */
    private BigDecimal getCurrentPrice(String symbolId, Long exchangeId, Long orgId) {
        try {
            GetRealtimeReply reply = grpcQuoteService.getRealtime(exchangeId, symbolId, orgId);
            BigDecimal currentPrice;
            if (Objects.nonNull(reply) && CollectionUtils.isNotEmpty(reply.getRealtimeList())) {
                Realtime realtime = reply.getRealtime(0);
                currentPrice = StringUtils.isNotEmpty(realtime.getC()) ? new BigDecimal(realtime.getC()) : BigDecimal.ZERO;
            } else {
                currentPrice = BigDecimal.ZERO;
            }
            return currentPrice;
        } catch (Exception e) {
            log.warn("getCurrentPrice error!", e);
            return BigDecimal.ZERO;
        }
    }

    public BigDecimal getCacheCurrentPrice(String symbolId, Long exchangeId, Long orgId) {
        SimpleSymbol simpleSymbol = new SimpleSymbol(exchangeId, symbolId, orgId);
        try {
            return quotePriceCache.get(simpleSymbol);
        } catch (ExecutionException e) {
            log.warn("getCacheCurrentPrice error!", e);
            return BigDecimal.ZERO;
        }
    }

    private static final class SimpleSymbol {
        private final Long exchangeId;
        private final String symbolId;
        private final Long orgId;

        public SimpleSymbol(Long exchangeId, String symbolId, Long orgId) {
            this.exchangeId = exchangeId;
            this.symbolId = symbolId;
            this.orgId = orgId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SimpleSymbol)) return false;
            SimpleSymbol that = (SimpleSymbol) o;
            return Objects.equals(getExchangeId(), that.getExchangeId()) &&
                    Objects.equals(getSymbolId(), that.getSymbolId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getExchangeId(), getSymbolId());
        }

        public Long getExchangeId() {
            return exchangeId;
        }

        public String getSymbolId() {
            return symbolId;
        }

        public Long getOrgId() {
            return orgId;
        }
    }

}
