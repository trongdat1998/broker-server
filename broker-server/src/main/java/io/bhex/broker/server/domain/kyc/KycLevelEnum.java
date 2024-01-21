package io.bhex.broker.server.domain.kyc;

public enum KycLevelEnum {
    NONE(0),
    BASIC(10),
    SENIOR_MANUAL(20),
    SENIOR_FACE(25),
    VIP(30);

    private final int value;

    KycLevelEnum(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
