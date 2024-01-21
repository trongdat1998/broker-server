package io.bhex.broker.server.domain;

public enum InviteStatisticsRecordStatus {

    DELETED(-1),
    WAITING(0),
    FINISHED(1);

    private int status;

    InviteStatisticsRecordStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }
}
