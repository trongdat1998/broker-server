package io.bhex.broker.server.domain;

public enum ApiKeyType {

    USER_API_KEY_READONLY(0), // 只读API
    USER_API_KEY_TRADABLE(1), // 可交易API
    ORG_API_KEY_READONLY(10), // 二级，允许操作用户资产
    ORG_API_KEY_EXECUTED(11); // 一级，允许统计查询、获取用户信息等

    private int type;

    ApiKeyType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }

    public static ApiKeyType forType(int type) {
        switch (type) {
            case 0:
                return USER_API_KEY_READONLY;
            case 1:
                return USER_API_KEY_TRADABLE;
            case 10:
                return ORG_API_KEY_READONLY;
            case 11:
                return ORG_API_KEY_EXECUTED;
            default:
                return null;
        }
    }

    public boolean isUserAPiKey() {
        return type() < 10;
    }

    public boolean isOrgAPiKey() {
        return type() >= 10;
    }

}
