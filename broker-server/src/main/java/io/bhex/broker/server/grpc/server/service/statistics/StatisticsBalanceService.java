package io.bhex.broker.server.grpc.server.service.statistics;

import io.bhex.broker.server.model.BalanceFlowSnapshot;
import io.bhex.broker.server.model.InsuranceFundBalanceSnap;
import io.bhex.broker.server.model.StatisticsBalanceSnapshot;
import io.bhex.broker.server.statistics.clear.mapper.StatisticsBalanceFlowMapper;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsBalanceSnapshotMapper;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsInsuranceFundBalanceMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @author wangsc
 * @description
 * @date 2020-04-17 15:23
 */
@Service
public class StatisticsBalanceService {
    @Resource
    private StatisticsBalanceFlowMapper statisticsBalanceFlowMapper;
    @Resource
    private StatisticsBalanceSnapshotMapper statisticsBalanceSnapshotMapper;
    @Resource
    private StatisticsInsuranceFundBalanceMapper statisticsInsuranceFundBalanceMapper;

    public List<StatisticsBalanceSnapshot> queryTokenBalanceSnapshotByDate(Long orgId, String statisticsTime, String tokenId) {
        return statisticsBalanceSnapshotMapper.queryTokenBalanceSnapshotByDate(orgId, statisticsTime, tokenId);
    }

    public BigDecimal queryTokenBalanceSnapshotCountByAccountId(Long orgId, Long accountId, String tokenId, Set<String> times) {
        return statisticsBalanceSnapshotMapper.queryTokenBalanceSnapshotCountByAccountId(accountId, tokenId, times);
    }

    public BigDecimal getTokenBalanceSnapshotTotalByAccountId(Long orgId, Long accountId, String tokenId, String startDate, String endDate) {
        return statisticsBalanceSnapshotMapper.getTokenBalanceSnapshotTotalByAccountId(accountId, tokenId, startDate, endDate);
    }

    public List<BalanceFlowSnapshot> queryAccountBalanceFlow(Long orgId, Long accountId, List<Integer> businessSubject, Long lastId, Integer limit, String tokenId) {
        return statisticsBalanceFlowMapper.queryAccountBalanceFlow(accountId, businessSubject, lastId, limit, tokenId);
    }

    public List<BalanceFlowSnapshot> queryComeInBalanceFlow(Long orgId, String tokenId, List<Long> accountIds, String start, String end, Integer limit) {
        return statisticsBalanceFlowMapper.queryComeInBalanceFlow(tokenId, accountIds, start, end, limit);
    }

    public List<BalanceFlowSnapshot> queryOutgoBalanceFlow(Long orgId, String tokenId, List<Long> accountIds, String start, String end, Integer limit) {
        return statisticsBalanceFlowMapper.queryOutgoBalanceFlow(tokenId, accountIds, start, end, limit);
    }

    public BigDecimal sumAccountChanged(Long orgId, Long repurchaseAccountId, int businessSubject, String tokenId) {
        return statisticsBalanceFlowMapper.sumAccountChanged(repurchaseAccountId, businessSubject, tokenId);
    }

    public List<InsuranceFundBalanceSnap> queryFutureInsuranceFundBalanceSnapList(Long orgId, Long exchangeId, Long accountId, String tokenId,
                                                                                  Date startDate, Date endDate, Integer limit) {
        boolean orderDesc = true;
        if (startDate == null && endDate != null) {
            orderDesc = false;
        }
        return statisticsInsuranceFundBalanceMapper.getBalanceSnapList(exchangeId, accountId, tokenId, startDate, endDate, limit, orderDesc);
    }

    public List<StatisticsBalanceSnapshot> queryAssetSnapByAccountIds(List<Long> accountIds, String tokenId, Long dt) {
        LocalDateTime startTime = LocalDateTime.ofInstant(new Date(dt).toInstant(), ZoneId.of("UTC"));
        String startTimeStr = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return statisticsBalanceSnapshotMapper.queryAssetSnapByAccountIds(accountIds, tokenId, startTimeStr);
    }

}
