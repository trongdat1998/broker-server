package io.bhex.broker.server.domain;


public enum PushMessageType {
    REGISTER("USER.REGISTER"),
    KYC("USER.KYC"),
    TRADE_SPOT("TRADE.DETAIL"),
    FUTURES_MESSAGE("FUTURES.MESSAGE"),
    OTC_TRADE("OTC.TRADE"),
    LOGIN("USER.LOGIN")
    ;

    private String value;

    PushMessageType(String value) {
        this.value = value;
    }

    public static PushMessageType fromValue(String value) {
        for (PushMessageType authType : PushMessageType.values()) {
            if (authType.value.equals(value)) {
                return authType;
            }
        }
        return null;
    }

    public String value() {
        return value;
    }
}
