package io.bhex.broker.server.domain;

public enum FinanceBusinessType {

    PURCHASE(1),
    REDEEM(2),
    INTEREST(3);

    private int type;

    FinanceBusinessType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
