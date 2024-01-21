package io.bhex.broker.server.domain;

public enum ConvertSymbolPriceType {
    // 价格类型： 1固定值，2浮动
    FIXED(1),
    FLOAT(2);

    private int type;

    ConvertSymbolPriceType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static ConvertSymbolPriceType fromType(int type) {
        for (ConvertSymbolPriceType at : ConvertSymbolPriceType.values()) {
            if (at.getType() == type) {
                return at;
            }
        }
        return null;
    }

}
