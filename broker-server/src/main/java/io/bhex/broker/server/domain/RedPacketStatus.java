package io.bhex.broker.server.domain;

public enum RedPacketStatus {
    DOING(0),
    OVER(1),
    REFUNDING(2),
    REFUNDED(3);

    private int status;

    RedPacketStatus(int status) {
        this.status = status;
    }

    public int status() {
        return status;
    }
}
