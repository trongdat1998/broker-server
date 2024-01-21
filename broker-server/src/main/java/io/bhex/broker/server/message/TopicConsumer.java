package io.bhex.broker.server.message;

/**
 * 可消费的主题
 *
 * @author wangshouchao
 */
public enum TopicConsumer {
    /**
     * 注意code值与TopicType对应
     */
    OTC_ORDER(3, "otcOrder"),
    TICKET_RECEIVE(4, "ticketReceive"),
    TRADE_DETAIL(5, "tradeDetail");

    private final int code;
    private final String name;

    TopicConsumer(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static TopicConsumer codeOf(int code) {
        for (TopicConsumer topicConsumer : values()) {
            if (topicConsumer.code == code) {
                return topicConsumer;
            }
        }
        return null;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

}
