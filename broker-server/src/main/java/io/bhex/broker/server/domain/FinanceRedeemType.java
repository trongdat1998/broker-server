package io.bhex.broker.server.domain;

public enum FinanceRedeemType {

    NORMAL(0),
    BIG(1);

    private int type;

    FinanceRedeemType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static FinanceRedeemType fromValue(int type) {
        for (FinanceRedeemType t : FinanceRedeemType.values()) {
            if (t.getType() == type) {
                return t;
            }
        }
        return null;
    }
}
