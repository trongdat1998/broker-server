package io.bhex.broker.server.domain;

public enum FinanceTransferStatus {

    FAIL(0),
    SUCCESS(1),
    EXCEPTION(2);

    private int status;

    FinanceTransferStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static FinanceTransferStatus fromValue(int status) {
        for (FinanceTransferStatus s : FinanceTransferStatus.values()) {
            if (s.getStatus() == status) {
                return s;
            }
        }
        return null;
    }
}
