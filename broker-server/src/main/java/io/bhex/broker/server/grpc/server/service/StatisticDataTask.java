package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.server.primary.mapper.StatisticDataMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author lizhen
 * @date 2018-12-05
 */
@Component
public class StatisticDataTask {

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private StatisticDataMapper statisticDataMapper;

}
