package io.bhex.broker.server.domain;

public enum PointReleaseParamType {

    NORMAL(0),
    DAILY(1),;

    private int type;

    PointReleaseParamType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
