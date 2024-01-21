package io.bhex.broker.server.domain;

public enum ActivityDrawStatus {

    NORMAL(0),
    USED(1),;

    private int status;

    ActivityDrawStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static ActivityDrawStatus fromType(int status) {
        for (ActivityDrawStatus at : ActivityDrawStatus.values()) {
            if (at.getStatus() == status) {
                return at;
            }
        }
        return null;
    }

}
