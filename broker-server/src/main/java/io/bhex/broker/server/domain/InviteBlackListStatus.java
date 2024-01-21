package io.bhex.broker.server.domain;

public enum InviteBlackListStatus {

    DELETED(-1),
    NORMAL(0);

    private int status;

    InviteBlackListStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }
}
