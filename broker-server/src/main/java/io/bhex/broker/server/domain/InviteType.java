package io.bhex.broker.server.domain;

public enum InviteType {

    DIRECT(1, "直接"),
    INDIRECT(2, "间接");

    InviteType(int value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    private int value;

    private String desc;

    public int getValue() {
        return value;
    }

    public String getDesc() {
        return desc;
    }
}
