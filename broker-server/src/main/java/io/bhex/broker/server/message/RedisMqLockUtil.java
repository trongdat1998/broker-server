package io.bhex.broker.server.message;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author wangsc 主要用于长时间的分布式锁持续竞争
 * @description redis锁(允许重入, 同时只能持有锁标志的用户线程才能释放)
 */
@Slf4j
@Component
public class RedisMqLockUtil {

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    private static final String LOCK_PRE = "REDIS_MQ_LOCK:";

    private final ConcurrentHashMap<String, Long> cacheLogPrint = new ConcurrentHashMap<>();

    /**
     * 基本不用清理
     */
    public void cleanCacheLogPrint() {
        long cleanTime = System.currentTimeMillis() - 2 * 3600 * 1000;
        cacheLogPrint.entrySet().stream().filter(a -> a.getValue() < cleanTime)
                .forEach(a -> cacheLogPrint.remove(a.getKey()));
    }

    private boolean setIfAbsent(String key, String uuid, int expireSeconds) {
        return Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(LOCK_PRE + key, uuid, expireSeconds, TimeUnit.SECONDS));
    }

    private String get(String key) {
        return redisTemplate.opsForValue().get(LOCK_PRE + key);
    }

    private boolean delete(String key) {
        return Boolean.TRUE.equals(redisTemplate.delete(LOCK_PRE + key));
    }

    private boolean hasKey(String key) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(LOCK_PRE + key));
    }

    private boolean expire(String key, int expireSeconds) {
        return Boolean.TRUE.equals(redisTemplate.expire(LOCK_PRE + key, expireSeconds, TimeUnit.SECONDS));
    }

    /**
     * 加锁
     *
     * @param key           加锁key
     * @param expireSeconds 锁的指定失效时间
     * @param retryTimes    尝试获取锁的次数
     * @param sleepMillis   尝试获取的间隔时间
     * @return 布尔
     */
    public boolean lock(String key, int expireSeconds, int retryTimes, long sleepMillis, String uuid) {
        if (key == null || uuid == null) {
            return false;
        }
        // 初次尝试获取
        boolean result;
        try {
            result = tryLock(key, expireSeconds, uuid);
            while ((!result) && retryTimes-- > 0) {
                if (!cacheLogPrint.containsKey(uuid)) {
                    //每2小时第一次请求失败打印日志
                    cacheLogPrint.put(uuid, System.currentTimeMillis());
                    log.info("Lock failed, retrying..." + retryTimes);
                }
                // 等待指定时间
                Thread.sleep(sleepMillis);
                // 再次获取
                result = tryLock(key, expireSeconds, uuid);
            }
        } catch (Exception e) {
            log.warn("Lock Error!" + key, e);
            result = false;
        }
        return result;
    }

    /**
     * 更新设置过期时间(只有持有分布式锁的线程才能更新)
     *
     * @param key           加锁key
     * @param expireSeconds 锁的指定失效时间
     * @param uuid          拥有者唯一标识
     * @return 布尔
     */
    public boolean updateExpire(String key, int expireSeconds, String uuid) {
        if (key == null || uuid == null) {
            return false;
        }
        boolean bl = false;
        try {
            String value = get(key);
            if (uuid.equals(value)) {
                bl = expire(key, expireSeconds);
            } else if (value == null) {
                //处理持有者在缓存脱档时，重新锁定缓存
                bl = tryLock(key, expireSeconds, uuid);
                log.info("ReTry lock!" + key);
            } else {
                log.info("The user not authority!" + key);
            }
        } catch (Exception e) {
            log.warn("UpdateExpire Lock Error!" + key, e);
        }
        return bl;
    }

    /**
     * 尝试Lock
     *
     * @param key           加速的key
     * @param expireSeconds 尝试锁定的时间（s）
     * @return 布尔
     */
    private boolean tryLock(String key, int expireSeconds, String uuid) {
        // setIfAbsent如果没有key的缓存，设置key的缓存
        boolean result = setIfAbsent(key, uuid, expireSeconds);
        if (!result) {
            //存在判断是否是自身，允许重入
            result = uuid.equals(get(key));
        } else {
            //如果抢到了,移除失败日志的避免重复
            cacheLogPrint.remove(uuid);
        }
        return result;
    }

    /**
     * 释放锁 有可能因为持锁之后方法执行时间大于锁的有效期，此时有可能已经被另外一个线程持有锁，所以不能直接删除
     *
     * @param key  加锁的key
     * @param uuid 当前拥有者唯一标识
     * @return false: 锁已不属于当前用户 或者 锁已超时
     */
    public boolean releaseLock(String key, String uuid) {
        if (key == null || uuid == null) {
            return false;
        }
        boolean bl = false;
        try {
            String value = get(key);
            if (uuid.equals(value)) {
                bl = delete(key);
            } else {
                log.info("The user not authority!" + key);
            }
        } catch (Exception e) {
            log.warn("Release Lock Error!" + key, e);
        }
        return bl;
    }

    /**
     * 查看是否加锁
     *
     * @param key 加速的key
     * @return 布尔
     */
    public boolean isLocked(String key) {
        if (key == null) {
            return false;
        }
        try {
            return hasKey(key);
        } catch (Exception e) {
            log.warn("IsLocked Error!" + key, e);
            return false;
        }
    }
}
