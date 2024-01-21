package io.bhex.broker.server.domain;

public enum  InviteBonusRecordStatus {

    WAITING(0),
    FINISHED(1);

    private int value;

    InviteBonusRecordStatus(Integer value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static InviteBonusRecordStatus fromValue(int value) {
        for (InviteBonusRecordStatus status : InviteBonusRecordStatus.values()) {
            if (status.value() == value) {
                return status;
            }
        }
        return null;
    }


}
