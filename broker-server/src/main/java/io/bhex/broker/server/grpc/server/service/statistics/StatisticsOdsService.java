package io.bhex.broker.server.grpc.server.service.statistics;

import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.statistics.clear.mapper.StatisticsClearOdsMapper;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsOdsMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.lang.reflect.Method;
import java.util.*;

/**
 * @author wangsc
 * @description 统计Ods服务
 * @date 2020-06-19 19:59
 */
@Service
@Slf4j
public class StatisticsOdsService {

    @Resource
    private StatisticsOdsMapper statisticsOdsMapper;

    @Resource
    private StatisticsClearOdsMapper statisticsClearOdsMapper;

    /**
     * 似乎有一定的风险（可以执行纯sql）
     *
     * @param bizKey
     * @param map
     * @return
     * @throws Exception
     */
    public List<HashMap<String, Object>> queryGroupDataListByBizKey(String bizKey, Map<String, Object> map) throws Exception {
        List<HashMap<String, Object>> list = new ArrayList<>();
        boolean existedMethod = Arrays.stream(statisticsOdsMapper.getClass().getMethods())
                .anyMatch(m -> m.getName().equals(bizKey));
        if (existedMethod) {
            Method method = statisticsOdsMapper.getClass().getMethod(bizKey, Map.class);
            list = (List<HashMap<String, Object>>) method.invoke(statisticsOdsMapper, map);
        }
        return list;
    }

    public List<HashMap<String, Object>> queryGroupDataList(String db, String execSql, long brokerId, String confKey, String unit, long start, long pageSize) {
        log.info("queryGroupDataList! db:{},sql:{}", db, execSql);
        if ("clear".equals(db)) {
            return statisticsClearOdsMapper.queryGroupDataList(execSql);
        } else {
            return statisticsOdsMapper.queryGroupDataList(execSql);
        }
    }

    public List<HashMap<String, Object>> queryGroupTokenDataList(String db, String execSql, long brokerId, String confKey, String unit, long start, long pageSize) {
        log.info("queryGroupTokenDataList! db:{},sql:{}", db, execSql);
        if ("clear".equals(db)) {
            return statisticsClearOdsMapper.queryGroupDataList(execSql);
        } else {
            return statisticsOdsMapper.queryGroupDataList(execSql);
        }
    }

    public List<HashMap<String, Object>> queryGroupTradeDataList(String execSql, long brokerId, String unit, long start, long pageSize, List<Symbol> symbols) {
        log.info("queryGroupTradeDataList! sql:{}", execSql);
        return statisticsClearOdsMapper.queryGroupDataList(execSql);
    }

    public HashMap<String, Object> queryFutureSymbol(Long orgId, String symbolId) {
        return statisticsClearOdsMapper.queryFutureSymbol(symbolId);
    }

}
