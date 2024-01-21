package io.bhex.broker.server.domain;

public enum WithdrawAddressStatus {

    NORMAL(0),
    DELETED(-1);

    private int status;

    WithdrawAddressStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
