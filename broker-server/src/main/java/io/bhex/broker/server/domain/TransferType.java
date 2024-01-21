package io.bhex.broker.server.domain;

public enum TransferType {
    ORG_API(1),
    USER_SELF(2);

    private int type;

    TransferType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }
}
