package io.bhex.broker.server.domain;

/**
 * 认购条件业务类型
 *
 * @author songxd
 * @date 2020-09-23
 */
public enum EnumBusinessLimit {
    /**
     * 理财产品
     */
    STAKING_PRODUCT(0);

    private final int code;
    EnumBusinessLimit(int code) {
        this.code = code;
    }
    public int code() {
        return code;
    }
}
