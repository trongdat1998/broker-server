package io.bhex.broker.server.domain;

public enum MessageType {
    COMMON(0, ""),
    FUTURES(1, "futures"); //期

    private final int value;
    private final String type;

    MessageType(int value, String type) {
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
