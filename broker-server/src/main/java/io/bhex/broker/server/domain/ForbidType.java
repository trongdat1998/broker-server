package io.bhex.broker.server.domain;



public enum ForbidType {
    CLOSE(0, "close"),
    OPEN(1, "open");

    private final int value;
    private final String type;

    ForbidType(int value, String type) {
        this.value = value;
        this.type = type;
    }

    public int value() {
        return this.value;
    }

    public String type() {
        return this.type;
    }
}
