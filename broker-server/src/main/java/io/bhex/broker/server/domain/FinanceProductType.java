package io.bhex.broker.server.domain;

public enum FinanceProductType {

    CURRENT(0), // 使用中的
    ;

    private int type;

    FinanceProductType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }

}
