package io.bhex.broker.server.domain;

public enum ConvertSymbolStatus {
    // 币对状态： 1启用，2禁用
    ENABLE(1),
    DISABLE(2);

    private int status;

    ConvertSymbolStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static ConvertSymbolStatus fromStatus(int status) {
        for (ConvertSymbolStatus at : ConvertSymbolStatus.values()) {
            if (at.getStatus() == status) {
                return at;
            }
        }
        return null;
    }

}
