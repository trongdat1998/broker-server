package io.bhex.broker.server.domain;

public enum ActivityDrawType {

    USER(1),
    SHARE(2),;

    private int type;

    ActivityDrawType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static ActivityDrawType fromType(int type) {
        for (ActivityDrawType at : ActivityDrawType.values()) {
            if (at.getType() == type) {
                return at;
            }
        }
        return null;
    }
}

