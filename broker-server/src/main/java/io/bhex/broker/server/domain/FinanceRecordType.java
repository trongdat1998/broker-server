package io.bhex.broker.server.domain;

public enum FinanceRecordType {

    PURCHASE(0), // 申购
    REDEEM(1), // 赎回
    INTEREST(2); // 利息发放

    private int type;

    FinanceRecordType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }

}
