package io.bhex.broker.server.domain;

public enum OtcThirdPartyStatus {

    DISABLE(0), // 禁用
    ENABLE(1); // 启用

    private int status;

    OtcThirdPartyStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static OtcThirdPartyStatus forStatus(int status) {
        switch (status) {
            case 0:
                return DISABLE;
            case 1:
                return ENABLE;
            default:
                return null;
        }
    }
}

