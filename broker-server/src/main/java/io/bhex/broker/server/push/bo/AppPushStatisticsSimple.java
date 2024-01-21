package io.bhex.broker.server.push.bo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wangsc
 * @description 统计缓存
 * @date 2020-07-30 15:15
 */
public final class AppPushStatisticsSimple {

    private final Long taskId;
    private final Long taskRound;
    /**
     * 触达数量
     */
    private final AtomicLong deliveryCount;
    /**
     * 链接点击数量
     */
    private final AtomicLong clickCount;
    /**
     * 有效数量
     */
    private final AtomicLong effectiveCount;
    /**
     * 退订数量
     */
    private final AtomicLong unsubscribeCount;
    /**
     * 卸载数量
     */
    private final AtomicLong uninstallCount;
    /**
     * 取消推送权限数量
     */
    private final AtomicLong cancelCount;

    /**
     * 过期时间
     */
    private final Long expireTime;

    public AppPushStatisticsSimple(Long taskId, Long taskRound, Long expireTime) {
        this.taskRound = taskRound;
        this.taskId = taskId;
        this.deliveryCount = new AtomicLong(0);
        this.effectiveCount = new AtomicLong(0);
        this.cancelCount = new AtomicLong(0);
        this.clickCount = new AtomicLong(0);
        this.uninstallCount = new AtomicLong(0);
        this.unsubscribeCount = new AtomicLong(0);
        this.expireTime = expireTime;
    }

    public AtomicLong getDeliveryCount() {
        return deliveryCount;
    }

    public AtomicLong getClickCount() {
        return clickCount;
    }

    public AtomicLong getEffectiveCount() {
        return effectiveCount;
    }

    public AtomicLong getUnsubscribeCount() {
        return unsubscribeCount;
    }

    public AtomicLong getUninstallCount() {
        return uninstallCount;
    }

    public AtomicLong getCancelCount() {
        return cancelCount;
    }

    public Long getTaskId() {
        return taskId;
    }

    public Long getTaskRound() {
        return taskRound;
    }

    public Long getExpireTime(){
        return expireTime;
    }
}
