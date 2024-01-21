package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.admin.FetchOneRequest;
import io.bhex.broker.server.primary.mapper.BrokerDBToolsMapper;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsDBToolsMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AdminBrokerDBToolsService {

    @Resource
    private BrokerDBToolsMapper brokerDBToolsMapper;

    @Resource
    private StatisticsDBToolsMapper statisticsDBToolsMapper;

    public String fetchOneBroker(FetchOneRequest request) {
        return fetchOne(request, "broker");
    }

    public String fetchOneStatistics(FetchOneRequest request) {
        return fetchOne(request, "statistics");
    }

    private String fetchOne(FetchOneRequest request, String type) {
        //表名不能为空
        if (StringUtils.isBlank(request.getTableName())) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        String[] fields;
        if (request.getFieldsCount() == 0) {
            fields = new String[]{"*"};
        } else {
            fields = request.getFieldsList().toArray(new String[0]);
        }
        List<Map<String,String>> conditions = request.getConditionsList().stream().map(item -> {
            Map<String,String> value = Maps.newHashMap();
            value.put("name", item.getName());
            value.put("condition", item.getCondition());
            value.put("value", item.getValue());
            return value;
        }).collect(Collectors.toList());
        if (type == "broker") {
            return new Gson().toJson(brokerDBToolsMapper.fetchOne(request.getTableName(), StringUtils.join(fields, ","), conditions));
        } else {
            return new Gson().toJson(statisticsDBToolsMapper.fetchOne(request.getTableName(), StringUtils.join(fields, ","), conditions));
        }

    }
}
