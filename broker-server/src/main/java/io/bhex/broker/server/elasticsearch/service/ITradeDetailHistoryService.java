package io.bhex.broker.server.elasticsearch.service;

import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import org.springframework.data.domain.Page;

import java.util.List;

public interface ITradeDetailHistoryService {
    List<TradeDetail> queryWithCondition(Header header, Long accountId, int queryDataType, Long orderId, String symbolId, String quoteTokenId,
                                         Integer orderSide, long startTime, long endTime, Long fromId, Long endId, int limit, boolean withFuturesInfo);

    Page<TradeDetail> queryPageWithCondition(Header header, Long accountId, int queryDataType, Long orderId, String symbolId, String quoteTokenId,
                                             Integer orderSide, long startTime, long endTime, Long fromId, Long endId, int page, int limit, boolean withFuturesInfo);
}
