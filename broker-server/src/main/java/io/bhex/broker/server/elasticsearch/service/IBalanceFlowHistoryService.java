package io.bhex.broker.server.elasticsearch.service;

import io.bhex.base.account.BusinessSubject;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.elasticsearch.entity.BalanceFlow;
import org.springframework.data.domain.Page;

import java.util.List;

public interface IBalanceFlowHistoryService {
    Page<BalanceFlow> queryPageWithCondition(Header header, Long accountId, List<String> tokenIds, List<BusinessSubject> businessSubjectList,
                                             long startTime, long endTime, Long fromId, Long endId, int page, int limit);

    List<BalanceFlow> queryWithCondition(Header header, Long accountId, List<String> tokenIds, List<BusinessSubject> businessSubjectList,
                                         long startTime, long endTime, Long fromId, Long endId, int limit);
}
