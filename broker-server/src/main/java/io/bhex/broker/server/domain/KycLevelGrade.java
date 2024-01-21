package io.bhex.broker.server.domain;

public enum KycLevelGrade {

    NONE(0),

    // 基础认证
    BASIC(1),

    // 高级认证
    SENIOR(2),

    // VIP认证
    VIP(3),

    UNKNOWN(-1);

    private final int value;

    KycLevelGrade(int value) {
        this.value = value;
    }

    public static KycLevelGrade fromKycLevel(int kycLevel) {
        int value = kycLevel / 10;
        for (KycLevelGrade grade : KycLevelGrade.values()) {
            if (grade.value == value) {
                return grade;
            }
        }

        return UNKNOWN;
    }

    public int getValue() {
        return value;
    }
}
