package io.bhex.broker.server.domain;

public enum InviteActivityType {

    TRADE(1, "交易"),
    CARD(2, "点卡"),
    REGISTER(3, "注册"),
    GENERAL_TRADE_FEEBACK(4, "新版交易返佣"),
    MARGE(10, "汇总");

    InviteActivityType(int value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    private int value;

    private String desc;

    public int getValue() {
        return value;
    }

    public String getDesc() {
        return desc;
    }

}
