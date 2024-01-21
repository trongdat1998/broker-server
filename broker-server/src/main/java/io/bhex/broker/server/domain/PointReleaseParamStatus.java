package io.bhex.broker.server.domain;

public enum PointReleaseParamStatus {

    WAITING(0),
    RUNNING(1),
    FINISHED(2);

    private int status;

    PointReleaseParamStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
