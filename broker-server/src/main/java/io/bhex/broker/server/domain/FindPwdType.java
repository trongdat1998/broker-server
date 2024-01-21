package io.bhex.broker.server.domain;

public enum FindPwdType {

    MOBILE(1),
    EMAIL(2),
    ;

    private int value;

    FindPwdType(int value) {
        this.value = value;
    }

    public static FindPwdType fromType(Integer value) {
        for (FindPwdType findPwdType : FindPwdType.values()) {
            if (findPwdType.value == value) {
                return findPwdType;
            }
        }
        return null;
    }

    public int value() {
        return this.value;
    }

}
