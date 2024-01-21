package io.bhex.broker.server.domain;

public enum  InviteGrantStatus {

    WAITING(0),
    EXECUTING(1),
    PART_FINISHED(2),
    FINISHED(3);

    private int value;

    InviteGrantStatus(Integer value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static InviteGrantStatus fromValue(int value) {
        for (InviteGrantStatus status : InviteGrantStatus.values()) {
            if (status.value() == value) {
                return status;
            }
        }
        return null;
    }

}
