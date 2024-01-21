package io.bhex.broker.server.elasticsearch.repository;

import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TradeDetailRepository extends ElasticsearchRepository<TradeDetail, Long> {
}
