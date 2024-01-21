package io.bhex.broker.server.domain;

public enum RedPacketType {

    NORMAL(0),
    RANDOM(1);

    private int type;

    RedPacketType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }
}
