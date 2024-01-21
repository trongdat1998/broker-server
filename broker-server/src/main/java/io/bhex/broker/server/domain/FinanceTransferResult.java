package io.bhex.broker.server.domain;

public enum  FinanceTransferResult {
    SUCCESS(0),
    FROM_ACCOUNT_NOT_EXIT(1),
    TO_ACCOUNT_NOT_EXIT(2),
    FROM_BALANCE_NOT_ENOUGH(3),
    PROCESSING(100),
    UNKNOWN_STATUS(999);

    private int code;
    FinanceTransferResult(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
