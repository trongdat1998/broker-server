package io.bhex.broker.server.domain;

public enum BalancePositionOperateType {
    LOCK(0),
    UNLOCK(1);

    private int type;

    BalancePositionOperateType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }
}
