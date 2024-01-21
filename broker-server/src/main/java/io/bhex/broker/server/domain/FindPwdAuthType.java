package io.bhex.broker.server.domain;

public enum FindPwdAuthType {

    NULL(0),
    MOBILE(1),
    EMAIL(2),
    GA(3),
    ID_CARD(4);

    private int value;

    FindPwdAuthType(int value) {
        this.value = value;
    }

    public static FindPwdAuthType fromValue(int value) {
        for (FindPwdAuthType authType : FindPwdAuthType.values()) {
            if (authType.value == value) {
                return authType;
            }
        }
        return null;
    }

    public int value() {
        return value;
    }

}
