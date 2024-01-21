package io.bhex.broker.server.grpc.server.service.statistics;

import io.bhex.broker.server.model.AgentCommission;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsAgentCommissionMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;

/**
 * @author zhangcb
 * @description
 */
@Service
public class StatisticsAgentService {

    @Resource
    private StatisticsAgentCommissionMapper statisticsAgentCommissionMapper;

    public List<AgentCommission> queryStatisticsAgentCommissionList(Long orgId, Long userId, String tokenId, String startTime, String endTime, Integer fromId, Integer endId, Integer limit) {
        Example example = Example.builder(AgentCommission.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", orgId);

        if (Objects.nonNull(userId) && userId > 0) {
            criteria.andEqualTo("userId", userId);
        }

        if (StringUtils.isNotEmpty(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }

        if (Objects.nonNull(fromId) && fromId > 0) {
            criteria.andLessThan("id", fromId);
        } else if (Objects.nonNull(endId) && endId > 0) {
            criteria.andGreaterThan("id", endId);
        }

        if (StringUtils.isNotEmpty(startTime)) {
            criteria.andGreaterThanOrEqualTo("dt", startTime);
        }

        if (StringUtils.isNotEmpty(endTime)) {
            criteria.andLessThanOrEqualTo("dt", endTime);
        }

        // 翻页信息
        if (Objects.isNull(limit) || limit > 100) {
            limit = 100;
        }
        return statisticsAgentCommissionMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }
}
