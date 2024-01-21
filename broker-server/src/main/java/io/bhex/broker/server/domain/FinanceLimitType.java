package io.bhex.broker.server.domain;

public enum FinanceLimitType {

    TOTAL_LIMIT(0),
    DAILY_LIMIT(1),
    USER_LIMIT(2);

    private int type;

    FinanceLimitType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static FinanceLimitType fromValue(int type) {
        for (FinanceLimitType t : FinanceLimitType.values()) {
            if (t.getType() == type) {
                return t;
            }
        }
        return null;
    }
}
