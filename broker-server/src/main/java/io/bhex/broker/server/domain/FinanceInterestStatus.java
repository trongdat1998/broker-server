package io.bhex.broker.server.domain;

public enum FinanceInterestStatus {

    UN_GRANT(0),
    FINISHED(1),
    DELETED(-1);

    private int status;

    FinanceInterestStatus(int status) {
        this.status = status;
    }

    public int status() {
        return status;
    }
}
