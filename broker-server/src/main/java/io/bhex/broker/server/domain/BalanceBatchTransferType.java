package io.bhex.broker.server.domain;

public enum BalanceBatchTransferType {
    TRANSFER(0),
    AIR_DROP(1),
    ;

    private int type;

    BalanceBatchTransferType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }
}
