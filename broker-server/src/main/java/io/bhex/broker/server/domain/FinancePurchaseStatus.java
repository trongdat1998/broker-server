package io.bhex.broker.server.domain;

public enum FinancePurchaseStatus {

    WAITING(0), // 等待处理
    FINISHED(1), // 已完结
    FAILED(3), // 申购失败，资产账户金额不足
    EXECUTE_EXCEPTION(10), // 异常，待处理
    DELETED(-1); // 异常，已终结

    private int status;

    FinancePurchaseStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static FinancePurchaseStatus fromValue(int status) {
        for (FinancePurchaseStatus s : FinancePurchaseStatus.values()) {
            if (s.getStatus() == status) {
                return s;
            }
        }
        return null;
    }

}
