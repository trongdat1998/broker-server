package io.bhex.broker.server.push.bo;

/**
 * 华为回执状态
 */
public enum HuaweiDeliveryStatusEnum {
    /**
     * 0成功,2没有应用,5非用户token,6通知栏限制展示，10非活跃的token，102超过每日推送上限，201推送管控
     */
    SUCCESS(0),
    NO_APP(2),
    NO_USER_TOKEN(5),
    NO_DISPLAY(6),
    NO_ACTIVE_TOKEN(10),
    MAX_PUSH_LIMIT(102),
    PUSH_CONTROL(201);
    private final int status;

    HuaweiDeliveryStatusEnum(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
