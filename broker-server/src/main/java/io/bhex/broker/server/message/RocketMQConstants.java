package io.bhex.broker.server.message;

import java.security.SecureRandom;

public final class RocketMQConstants {
    public static final String MQ_NOTIFY_BROKER_GROUP_DEFAULT = "broker_notify_message_group";
    public static final String MQ_NOTIFY_BROKER_TOPIC = "BhNotifyBroker";
    /**
     * 随机的节点code值
     */
    public static final int POD_CODE = new SecureRandom().nextInt(10000);
    public static final String MQ_NOTIFY_TRADE_TOPIC = "BhNotifyBroker";
}
