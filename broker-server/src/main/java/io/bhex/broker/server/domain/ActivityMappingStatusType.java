package io.bhex.broker.server.domain;


/**
 * 均摊处理状态枚举
 */
public enum ActivityMappingStatusType {
    PENDING(0),
    SOLVED(1),
    FAIL(2);

    private final int value;

    ActivityMappingStatusType(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }
}
