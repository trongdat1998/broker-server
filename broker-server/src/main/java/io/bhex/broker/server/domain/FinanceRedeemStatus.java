package io.bhex.broker.server.domain;

public enum FinanceRedeemStatus {

    WAITING(0), // 待处理
    FINISHED(1), // 已完结
    BIG_WAITING(2), // 大额待处理
    EXECUTE_EXCEPTION(10), // 异常，待处理
    DELETED(-1); // 异常，已完结

    private int status;

    FinanceRedeemStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static FinanceRedeemStatus fromValue(int status) {
        for (FinanceRedeemStatus s : FinanceRedeemStatus.values()) {
            if (s.getStatus() == status) {
                return s;
            }
        }
        return null;
    }
}
