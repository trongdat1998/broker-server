package io.bhex.broker.server.elasticsearch.repository;

import io.bhex.broker.server.elasticsearch.entity.BalanceFlow;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BalanceFlowRepository extends ElasticsearchRepository<BalanceFlow, Long> {
}
