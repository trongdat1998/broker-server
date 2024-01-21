package io.bhex.broker.server.grpc.server.service.statistics;

import io.bhex.broker.server.model.StatisticsTradeAllDetail;
import io.bhex.broker.server.model.SymbolTradeInfo;
import io.bhex.broker.server.statistics.clear.mapper.StatisticsTradeDetailMapper;
import io.bhex.broker.server.util.PageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

/**
 * @author wangsc
 * @description
 * @date 2020-04-17 15:34
 */
@Service
@Slf4j
public class StatisticsTradeService {
    @Resource
    private StatisticsTradeDetailMapper statisticsTradeDetailMapper;

    public List<SymbolTradeInfo> querySymbolTradeInfo(Long orgId, String symbolId, Timestamp startTime, Timestamp endTime) {
        //TODO 后期处理时间
        return statisticsTradeDetailMapper.querySymbolTradeInfo(orgId, symbolId, startTime, endTime);
    }

    public List<StatisticsTradeAllDetail> queryTradeDetailByOrgId(Long orgId, String time, int page, int size) {
        return statisticsTradeDetailMapper.queryTradeDetailByOrgId(orgId, time, page, size);
    }

    public List<StatisticsTradeAllDetail> queryTradeAllDetailByStartTime(Long orgId, Long accountId, List<String> symbolId, String startTime) {
        Example example = Example.builder(StatisticsTradeAllDetail.class).build();
        //获取交易量
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", orgId);
        criteria.andEqualTo("accountId", accountId);
        criteria.andIn("symbolId", symbolId);
        criteria.andGreaterThan("updatedAt", startTime);
        return statisticsTradeDetailMapper.selectByExample(example);
    }

    public StatisticsTradeAllDetail findTradeDetailNearBy(long orgId, long tradeDetailId, String symbolId) {
        //查找指定trade附近的指定币对的当时汇率
        Example example = Example.builder(StatisticsTradeAllDetail.class)
                .orderByDesc("tradeDetailId")
                .build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", orgId);
        criteria.andLessThan("tradeDetailId", tradeDetailId);
        criteria.andEqualTo("symbolId", symbolId);
        List<StatisticsTradeAllDetail> list = statisticsTradeDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(PageUtil.getStartIndex(1, 1), 1));
        StatisticsTradeAllDetail ref = CollectionUtils.isEmpty(list) ? null : list.get(0);
        log.info("tradeid:{} symbol:{} refrate:{}", tradeDetailId, symbolId, ref != null ? ref.getPrice() : BigDecimal.ZERO);
        return ref;
    }

    public List<StatisticsTradeAllDetail> findTradeDetailByIndex(long orgId, long startTradeId, long endTradeId, int index, int limit) {
        Example example = new Example(StatisticsTradeAllDetail.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", orgId);
        criteria.andGreaterThanOrEqualTo("tradeDetailId", startTradeId);
        if (endTradeId > 0) {
            criteria.andLessThanOrEqualTo("tradeDetailId", endTradeId);
        }
        return statisticsTradeDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(PageUtil.getStartIndex(index, limit), limit));
    }

    public List<StatisticsTradeAllDetail> findTradeDetailByIndexAndAccounts(long orgId, long startTradeId, long endTradeId, List<Long> accounts, int index, int limit) {
        Example example = Example.builder(StatisticsTradeAllDetail.class).orderByAsc("tradeDetailId").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", orgId);
        criteria.andGreaterThanOrEqualTo("tradeDetailId", startTradeId);
        if (endTradeId > 0) {
            criteria.andLessThanOrEqualTo("tradeDetailId", endTradeId);
        }
        criteria.andIn("accountId", accounts);
        return statisticsTradeDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(PageUtil.getStartIndex(index, limit), limit));
    }

    public StatisticsTradeAllDetail findTradeDetailByPrimaryKey(long orgId, long tradeDetailId) {
        return this.statisticsTradeDetailMapper.selectByPrimaryKey(tradeDetailId);
    }

}
