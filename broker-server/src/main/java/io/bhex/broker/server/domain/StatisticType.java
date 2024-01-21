package io.bhex.broker.server.domain;

/**
 * @author lizhen
 * @date 2018-12-05
 */
public enum StatisticType {
    NONE(0, "none");

    private int type;

    private String desc;

    StatisticType(int type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public int getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }
}
