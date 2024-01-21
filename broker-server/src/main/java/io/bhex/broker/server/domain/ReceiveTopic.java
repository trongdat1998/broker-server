package io.bhex.broker.server.domain;

public enum ReceiveTopic {

    HEARTBEAT("HeartBeat"),
    ORDER("Order"),
    TRADE("Trade"),
    BALANCE_DETAIL("BalanceDetail"),
    BALANCE_MESSAGE("BalanceMessage");

    private String topic;

    ReceiveTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
