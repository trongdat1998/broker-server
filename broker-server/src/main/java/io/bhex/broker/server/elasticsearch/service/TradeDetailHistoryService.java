package io.bhex.broker.server.elasticsearch.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import io.bhex.broker.server.elasticsearch.entity.TradeDetailFutures;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TradeDetailHistoryService implements ITradeDetailHistoryService {

    public static final String TRADE_DETAIL_INDEX_PREFIX = "trade_detail_";
    public static final String TRADE_DETAIL_FUTURES_INDEX_PREFIX = "trade_detail_futures_";
    public static final int MAX_HISTORY_DAYS = 90;
    public static final int NO_CURRENT_DAY_INDICES_MILLI_SECONDS = 7600000;

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Override
    public Page<TradeDetail> queryPageWithCondition(Header header, Long accountId, int queryDataType, Long orderId,
                                                    String symbolId, String quoteTokenId, Integer orderSide, long startTime, long endTime,
                                                    Long fromId, Long endId,
                                                    int page, int limit, boolean withFuturesInfo) {
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        QueryBuilder queryBuilder = getQueryCondition(header, accountId, queryDataType, orderId, symbolId, quoteTokenId, orderSide, startTime, endTime, fromId, endId);
        List<String> indicesList = getIndices(startTime, endTime, false);
        if (indicesList == null) {
            return Page.empty();
        }
        Query searchQuery = new NativeSearchQueryBuilder()
//                .withIndices(indicesList.toArray(new String[0]))
                .withIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .withQuery(queryBuilder)
                .withPageable(PageRequest.of(page, limit, getSort(header, fromId, endId)))
                .build();
        Page<TradeDetail> tradeDetailPage = elasticsearchRestTemplate.queryForPage(searchQuery, TradeDetail.class, IndexCoordinates.of(indicesList.toArray(new String[0])));
        if (withFuturesInfo) {
            List<TradeDetail> tradeDetailList = tradeDetailPage.getContent();
            List<TradeDetailFutures> tradeDetailFuturesList = queryFuturesTradeDetails(
                    tradeDetailList.stream().map(TradeDetail::getTradeDetailId).collect(Collectors.toList()),
                    getIndices(startTime, endTime, true));
            Map<Long, TradeDetailFutures> tradeDetailFuturesMap = tradeDetailFuturesList.stream()
                    .collect(Collectors.toMap(TradeDetailFutures::getTradeDetailId, tradeDetailFutures -> tradeDetailFutures));
            tradeDetailPage = tradeDetailPage.map(tradeDetail -> tradeDetail.toBuilder().tradeDetailFutures(tradeDetailFuturesMap.get(tradeDetail.getTradeDetailId())).build());
        }
        return tradeDetailPage;
    }

    @Override
    public List<TradeDetail> queryWithCondition(Header header, Long accountId, int queryDataType, Long orderId,
                                                String symbolId, String quoteTokenId, Integer orderSide, long startTime, long endTime,
                                                Long fromId, Long endId, int limit, boolean withFuturesInfo) {
        try {
            long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
            if (header.getOrgId() == 7070 && (startTime == 0 || startTime < sevenDaysBefore)) {
                startTime = sevenDaysBefore;
            }
            QueryBuilder queryBuilder = getQueryCondition(header, accountId, queryDataType, orderId, symbolId, quoteTokenId, orderSide, startTime, endTime, fromId, endId);
            List<String> indicesList = getIndices(startTime, endTime, false);
            if (indicesList == null) {
                log.warn("trade_detail_es_query get null indices, startTime:{}, endTime:{}", startTime, endTime);
                return Lists.newArrayList();
            }
            NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder()
//                    .withIndices(indicesList.toArray(new String[0]))
                    .withIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                    .withQuery(queryBuilder);
            if (orderId != null && orderId > 0) {
                long totalElements = elasticsearchRestTemplate.count(nativeSearchQueryBuilder.build(), IndexCoordinates.of(indicesList.toArray(new String[0])));
                if (totalElements == 0) {
                    return Lists.newArrayList();
                }
                nativeSearchQueryBuilder = nativeSearchQueryBuilder.withPageable(PageRequest.of(0, (int) totalElements, Sort.by(Sort.Order.asc("trade_detail_id"))));
            } else {
                nativeSearchQueryBuilder = nativeSearchQueryBuilder.withPageable(PageRequest.of(0, limit, getSort(header, fromId, endId)));
            }
            Query searchQuery = nativeSearchQueryBuilder.build();
            List<TradeDetail> tradeDetailList = elasticsearchRestTemplate.queryForList(searchQuery, TradeDetail.class, IndexCoordinates.of(indicesList.toArray(new String[0])));
//            log.info("trade_detail_es_query: " +
//                            "condition:[orgId:{}, userId:{}, accountId:{}, queryDataType:{}, orderId:{}, symbolId:{}, quoteTokenId:{}, orderSide:{}, startTime:{}, endTime:{}, fromId:{}, endId:{}], \ndata:{}, data size:{}",
//                    header.getOrgId(), header.getUserId(), accountId, queryDataType, orderId, symbolId, quoteTokenId, orderSide, startTime, endTime, fromId, endId, JsonUtil.defaultGson().toJson(tradeDetailList), tradeDetailList.size());
            if (withFuturesInfo) {
                indicesList = getIndices(startTime, endTime, true);
                List<TradeDetailFutures> tradeDetailFuturesList = queryFuturesTradeDetails(
                        tradeDetailList.stream().map(TradeDetail::getTradeDetailId).collect(Collectors.toList()), indicesList);
//                log.info("queryFuturesMatchInfo indices:{}", JsonUtil.defaultGson().toJson(indicesList));
                Map<Long, TradeDetailFutures> tradeDetailFuturesMap = tradeDetailFuturesList.stream()
                        .collect(Collectors.toMap(TradeDetailFutures::getTradeDetailId, tradeDetailFutures -> tradeDetailFutures));
                tradeDetailList = tradeDetailList.stream()
                        .map(tradeDetail ->
                        {
                            if (tradeDetailFuturesMap.get(tradeDetail.getTradeDetailId()) == null) {
                                log.error("cannot find tradeDetailFutures data with tradeId:{}", tradeDetail.getTradeDetailId());
                            }
                            return tradeDetail.toBuilder().isFuturesTrade(withFuturesInfo).tradeDetailFutures(tradeDetailFuturesMap.get(tradeDetail.getTradeDetailId())).build();
                        }).collect(Collectors.toList());
            }
            return Lists.newArrayList(tradeDetailList);
        } catch (Exception e) {
            log.error("queryTradeDetailWithCondition:[orgId:{}, userId:{}, accountId:{}, queryDataType:{}, symbolId:{}, quoteTokenId:{}, orderSide:{}, startTime:{}, endTime:{}, fromId:{}, endId:{}, limit:{}] error",
                    header.getOrgId(), header.getUserId(), accountId, queryDataType, symbolId, quoteTokenId, orderSide, startTime, endTime, fromId, endId, limit, e);
            throw e;
        }
    }

    private List<TradeDetailFutures> queryFuturesTradeDetails(List<Long> tradeDetailIds, List<String> indicesList) {
        if (indicesList == null || CollectionUtils.isEmpty(indicesList)) {
            return Lists.newArrayList();
        }
        if (tradeDetailIds == null || CollectionUtils.isEmpty(tradeDetailIds)) {
            return Lists.newArrayList();
        }
//        log.info("trade_detail_futures_es_query: tradeDetailIds:{}, indices:{}", JsonUtil.defaultGson().toJson(indicesList), JsonUtil.defaultGson().toJson(indicesList));
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("trade_detail_id", tradeDetailIds));
        Query searchQuery = new NativeSearchQueryBuilder()
//                .withIndices(indicesList.toArray(new String[0]))
                .withIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .withQuery(queryBuilder)
                .withPageable(PageRequest.of(0, tradeDetailIds.size()))
                .build();
        return elasticsearchRestTemplate.queryForList(searchQuery, TradeDetailFutures.class, IndexCoordinates.of(indicesList.toArray(new String[0])));
    }

    private QueryBuilder getQueryCondition(Header header, Long accountId, int queryDataType, Long orderId,
                                           String symbolId, String quoteTokenId, Integer orderSide, long startTime, long endTime,
                                           Long fromId, Long endId) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("broker_id", header.getOrgId()));
        if (accountId != null && accountId > 0) {
            queryBuilder.must(QueryBuilders.matchQuery("account_id", accountId));
        } else if (header.getUserId() > 0) {
            queryBuilder.must(QueryBuilders.matchQuery("broker_user_id", header.getUserId()));
        }

        if (orderId != null && orderId > 0) {
            queryBuilder.must(QueryBuilders.matchQuery("order_id", orderId));
        }

        if (!Strings.isNullOrEmpty(symbolId)) {
            queryBuilder.must(QueryBuilders.matchQuery("symbol_id.keyword", symbolId));
        } else {
            if (queryDataType > 0) {
                if (queryDataType == 1) {
                    queryBuilder.must(QueryBuilders.termsQuery("security_type", Lists.newArrayList(0, 1)));
                } else {
                    queryBuilder.must(QueryBuilders.matchQuery("security_type", queryDataType));
                }
            }
            if (!Strings.isNullOrEmpty(quoteTokenId)) {
                queryBuilder.must(QueryBuilders.matchQuery("quote_token_id.keyword", quoteTokenId));
            }
        }

        if (orderSide != null) {
            queryBuilder.must(QueryBuilders.matchQuery("side", orderSide));
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (startTime > 0 && endTime > 0) {
            queryBuilder.must(QueryBuilders.rangeQuery("created_at").format("yyyy-MM-dd HH:mm:ss").from(dateFormat.format(new Date(startTime))).to(dateFormat.format(new Date(endTime))));
        } else {
            if (startTime > 0) {
                queryBuilder.must(QueryBuilders.rangeQuery("created_at").format("yyyy-MM-dd HH:mm:ss").gte(dateFormat.format(new Date(startTime))));
            } else if (endTime > 0) {
                queryBuilder.must(QueryBuilders.rangeQuery("created_at").format("yyyy-MM-dd HH:mm:ss").lte(dateFormat.format(new Date(endTime))));
            }
        }
        if (fromId > 0 && endId > 0) {
            queryBuilder.must(QueryBuilders.rangeQuery("trade_detail_id").from(endId, false).to(fromId, false));
        } else {
            if (fromId > 0) {
                queryBuilder.must(QueryBuilders.rangeQuery("trade_detail_id").lt(fromId));
            } else if (endId > 0) {
                queryBuilder.must(QueryBuilders.rangeQuery("trade_detail_id").gt(endId));
            }
        }
        return queryBuilder;
    }

    private List<String> getIndices(long startTime, long endTime, boolean isFuturesIndices) {
        List<String> indicesList = Lists.newArrayList();
        LocalDateTime minStartDate = LocalDateTime.now().minusDays(MAX_HISTORY_DAYS);
        LocalDateTime startDate = null, endDate = null;
        if (startTime == 0 && endTime == 0) {
            startDate = minStartDate;
            endDate = LocalDateTime.now();
        } else if (startTime > 0 && endTime == 0) {
            startDate = LocalDateTime.ofInstant(new Date(startTime).toInstant(), ZoneId.systemDefault());
            endDate = LocalDateTime.now();
            if (Duration.between(endDate, startDate).toDays() > MAX_HISTORY_DAYS) {
                startDate = minStartDate;
            }
        } else if (startTime == 0 && endTime > 0) {
            startDate = minStartDate;
            endDate = LocalDateTime.ofInstant(new Date(endTime).toInstant(), ZoneId.systemDefault());
            if (endDate.isAfter(LocalDateTime.now())) {
                endDate = LocalDateTime.now();
            }
            if (endDate.isBefore(startDate)) {
                return null;
            }
        } else {
            startDate = LocalDateTime.ofInstant(new Date(startTime).toInstant(), ZoneId.systemDefault());
            if (startDate.isAfter(LocalDateTime.now())) {
                return null;
            }
            if (Duration.between(LocalDateTime.now(), startDate).toDays() > MAX_HISTORY_DAYS) {
                startDate = minStartDate;
            }
            endDate = LocalDateTime.ofInstant(new Date(endTime).toInstant(), ZoneId.systemDefault());
            if (endDate.isAfter(LocalDateTime.now())) {
                endDate = LocalDateTime.now();
            }
            if (endDate.isBefore(startDate)) {
                return null;
            }
        }
        // 排除当前不存在的indices
//        DateTime zeroTimeMilliSeconds = DateTime.now().withTime(0, 0, 0, 0);
        long currentMilliSeconds = System.currentTimeMillis();
        if (isFuturesIndices) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMM");
            for (; endDate.isAfter(startDate); endDate = endDate.minusMonths(1)) {
                indicesList.add(TRADE_DETAIL_FUTURES_INDEX_PREFIX + endDate.format(formatter));
            }
            if (!indicesList.contains(TRADE_DETAIL_FUTURES_INDEX_PREFIX + startDate.format(formatter))) {
                indicesList.add(TRADE_DETAIL_FUTURES_INDEX_PREFIX + startDate.format(formatter));
            }
//            if (zeroTimeMilliSeconds.getDayOfMonth() == 0 && zeroTimeMilliSeconds.getMillis() < currentMilliSeconds && currentMilliSeconds < zeroTimeMilliSeconds.getMillis() + NO_CURRENT_DAY_INDICES_MILLI_SECONDS) {
//                String currentDayIndices = TRADE_DETAIL_FUTURES_INDEX_PREFIX + LocalDateTime.now().format(formatter);
//                indicesList.remove(currentDayIndices);
//            }
        } else {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            for (; endDate.isAfter(startDate); endDate = endDate.minusDays(1)) {
                indicesList.add(TRADE_DETAIL_INDEX_PREFIX + endDate.format(formatter));
            }
            if (!indicesList.contains(TRADE_DETAIL_INDEX_PREFIX + startDate.format(formatter))) {
                indicesList.add(TRADE_DETAIL_INDEX_PREFIX + startDate.format(formatter));
            }
//            if (zeroTimeMilliSeconds.getMillis() < currentMilliSeconds && currentMilliSeconds < zeroTimeMilliSeconds.getMillis() + NO_CURRENT_DAY_INDICES_MILLI_SECONDS) {
//                indicesList.remove(TRADE_DETAIL_INDEX_PREFIX + LocalDateTime.now().format(formatter));
//            }
        }

        return indicesList.stream().distinct().collect(Collectors.toList());
    }

    public Sort getSort(Header header, Long fromId, Long endId) {
        Sort sort = Sort.by(Sort.Order.desc("trade_detail_id"));
        if (fromId == 0 && endId > 0) {
            sort = Sort.by(Sort.Order.asc("trade_detail_id"));
        }
        return sort;
    }

}
