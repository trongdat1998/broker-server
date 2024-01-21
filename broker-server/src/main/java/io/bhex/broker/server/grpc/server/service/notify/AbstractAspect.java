package io.bhex.broker.server.grpc.server.service.notify;


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

@Slf4j
public abstract class AbstractAspect {

    protected final static String TOPIC = "broker";

    //@Value("${spring.rocketmq.runEnv}")
    protected String runEnv;

    @Resource
    protected Environment environment;

/*    @Resource
    protected BrokerMessageProducer messageProducer;*/

    @Resource(name = "stringRedisTemplate")
    protected StringRedisTemplate redisTemplate;

    protected List<String> prodProfile=Lists.newArrayList("bhop","bhex");

    @PostConstruct
    public void init() {

        if (environment.getActiveProfiles().length == 0) {
            log.info("runEnv=null");
        } else {
            runEnv = environment.getActiveProfiles()[0].split("_")[0];
            log.info("runEnv={}", runEnv);
        }

        if (StringUtils.isEmpty(runEnv)) {
            throw new IllegalArgumentException("Miss runEnv");
        }

    }

    protected String buildKey(Object... fields) {
        List<Object> list = Lists.newArrayList(runEnv);
        list.addAll(Lists.newArrayList(fields));
        return Joiner.on("-").join(Lists.newArrayList(list));
    }

    protected String buildNotifyKey(String tag, String brokerId) {
        return buildKey(tag, "notify", brokerId);
    }

    protected void cacheNotify(String tag, String brokerId) {
        String notifyKey = buildNotifyKey(tag, brokerId);
        redisTemplate.opsForValue().setIfAbsent(notifyKey, "notify");
    }
}
