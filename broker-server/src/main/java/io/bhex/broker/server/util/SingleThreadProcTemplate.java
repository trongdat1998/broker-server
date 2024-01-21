package io.bhex.broker.server.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 单线程处理模版类
 * author: wangjun
 */
@Slf4j
public abstract class SingleThreadProcTemplate {

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    public void execExclusive(Map<String, String> param) {

        String key = buildExclusiveLockKey(param);
        boolean locked = RedisLockUtils.tryLock(redisTemplate, key, getExpire());
        if (!locked) {
            return;
        }

        try {
            exec(param);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, key);
        }
    }

    protected abstract String buildExclusiveLockKey(Map<String, String> param);

    protected abstract void exec(Map<String, String> param);

    protected abstract int getExpire();

}
