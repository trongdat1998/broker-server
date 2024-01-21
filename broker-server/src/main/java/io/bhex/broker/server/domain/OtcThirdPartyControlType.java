package io.bhex.broker.server.domain;

public enum OtcThirdPartyControlType {

    NOT_CONTROL(0), // 不判断可用，不锁仓
    CHECK_POSITION(1), // 判断可用，不锁仓
    LOCK_POSITION(2); // 判断可用，并锁仓

    private int type;

    OtcThirdPartyControlType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static OtcThirdPartyControlType forType(int type) {
        switch (type) {
            case 0:
                return NOT_CONTROL;
            case 1:
                return CHECK_POSITION;
            case 2:
                return LOCK_POSITION;
            default:
                return null;
        }
    }

}
