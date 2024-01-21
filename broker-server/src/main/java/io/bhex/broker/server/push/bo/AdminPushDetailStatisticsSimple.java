package io.bhex.broker.server.push.bo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wangsc
 * @description 详情统计
 * @date 2020-08-04 14:39
 */
public class AdminPushDetailStatisticsSimple {

    private final String reqOrderId;
    /**
     * 触达数量
     */
    private final AtomicLong deliveryCount;
    /**
     * 链接点击数量
     */
    private final AtomicLong clickCount;

    /**
     * 过期时间
     */
    private final Long expireTime;

    public AdminPushDetailStatisticsSimple(String reqOrderId, Long expireTime) {
        this.reqOrderId = reqOrderId;
        this.deliveryCount = new AtomicLong(0);
        this.clickCount = new AtomicLong(0);
        this.expireTime = expireTime;
    }

    public String getReqOrderId() {
        return reqOrderId;
    }

    public AtomicLong getDeliveryCount() {
        return deliveryCount;
    }

    public AtomicLong getClickCount() {
        return clickCount;
    }

    public Long getExpireTime() {
        return expireTime;
    }
}
