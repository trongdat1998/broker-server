package io.bhex.broker.server.elasticsearch.service;

import com.google.common.collect.Lists;
import io.bhex.base.account.BusinessSubject;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.elasticsearch.entity.BalanceFlow;
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
import java.util.stream.Collectors;

@Slf4j
@Service
public class BalanceFlowHistoryService implements IBalanceFlowHistoryService {

    public static final String BALANCE_FLOW_INDEX_PREFIX = "balance_flow_";
    public static final int MAX_HISTORY_DAYS = 90;
    public static final int NO_CURRENT_DAY_INDICES_MILLI_SECONDS = 7600000;

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    public Page<BalanceFlow> queryPageWithCondition(Header header, Long accountId, List<String> tokenIds, List<BusinessSubject> businessSubjectList,
                                                    long startTime, long endTime, Long fromId, Long endId, int page, int limit) {
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        QueryBuilder queryBuilder = getQueryCondition(header, accountId, tokenIds, businessSubjectList, startTime, endTime, fromId, endId);
        List<String> indicesList = getIndices(startTime, endTime);
        if (indicesList == null) {
            return Page.empty();
        }
        Query searchQuery = new NativeSearchQueryBuilder()
//                .withIndices(indicesList.toArray(new String[0]))
                .withIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .withQuery(queryBuilder)
                .withPageable(PageRequest.of(page, limit, getSort(header, fromId, endId)))
                .build();
//        log.info("queryBalanceFlow indices:{}", JsonUtil.defaultGson().toJson(indicesList));
        return elasticsearchRestTemplate.queryForPage(searchQuery, BalanceFlow.class, IndexCoordinates.of(indicesList.toArray(new String[0])));
    }

    public List<BalanceFlow> queryWithCondition(Header header, Long accountId, List<String> tokenIds, List<BusinessSubject> businessSubjectList,
                                                long startTime, long endTime, Long fromId, Long endId, int limit) {
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        QueryBuilder queryBuilder = getQueryCondition(header, accountId, tokenIds, businessSubjectList, startTime, endTime, fromId, endId);
        List<String> indicesList = getIndices(startTime, endTime);
        if (indicesList == null) {
            return Lists.newArrayList();
        }

        Query searchQuery = new NativeSearchQueryBuilder()
//                .withIndices(indicesList.toArray(new String[0]))
                .withIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .withQuery(queryBuilder)
                .withPageable(PageRequest.of(0, limit, getSort(header, fromId, endId)))
                .build();
        return Lists.newArrayList(elasticsearchRestTemplate.queryForList(searchQuery, BalanceFlow.class, IndexCoordinates.of(indicesList.toArray(new String[0]))));
    }

    private QueryBuilder getQueryCondition(Header header, Long accountId, List<String> tokenIds, List<BusinessSubject> businessSubjectList,
                                           long startTime, long endTime, Long fromId, Long endId) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("org_id", header.getOrgId()));
        if (accountId != null && accountId > 0) {
            queryBuilder.must(QueryBuilders.matchQuery("account_id", accountId));
        } else if (header.getUserId() > 0) {
            queryBuilder.must(QueryBuilders.matchQuery("broker_user_id", header.getUserId()));
        }

        if (!CollectionUtils.isEmpty(tokenIds)) {
            if (tokenIds.size() == 1) {
                queryBuilder.must(QueryBuilders.matchQuery("token_id.keyword", tokenIds.get(0)));
            } else {
                queryBuilder.must(QueryBuilders.termsQuery("token_id.keyword", tokenIds));
            }
        }
        if (!CollectionUtils.isEmpty(businessSubjectList)) {
            queryBuilder.must(QueryBuilders.termsQuery("business_subject", businessSubjectList.stream().map(BusinessSubject::getNumber).collect(Collectors.toList())));
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
            queryBuilder.must(QueryBuilders.rangeQuery("balance_flow_id").from(endId, false).to(fromId, false));
        } else {
            if (fromId > 0) {
                queryBuilder.must(QueryBuilders.rangeQuery("balance_flow_id").lt(fromId));
            } else if (endId > 0) {
                queryBuilder.must(QueryBuilders.rangeQuery("balance_flow_id").gt(endId));
            }
        }
        return queryBuilder;
    }

    private List<String> getIndices(long startTime, long endTime) {
        List<String> indicesList = Lists.newArrayList();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
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

        for (; endDate.isAfter(startDate); endDate = endDate.minusDays(1)) {
            indicesList.add(BALANCE_FLOW_INDEX_PREFIX + endDate.format(formatter));
        }
        if (!indicesList.contains(BALANCE_FLOW_INDEX_PREFIX + startDate.format(formatter))) {
            indicesList.add(BALANCE_FLOW_INDEX_PREFIX + startDate.format(formatter));
        }
        return indicesList;
    }

    public Sort getSort(Header header, Long fromId, Long endId) {
        Sort sort = Sort.by(Sort.Order.desc("balance_flow_id"));
        if (fromId == 0 && endId > 0) {
            sort = Sort.by(Sort.Order.asc("balance_flow_id"));
        }
        return sort;
    }

}
