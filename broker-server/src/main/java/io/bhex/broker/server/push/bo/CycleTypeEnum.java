package io.bhex.broker.server.push.bo;

/**
 * 循环类型
 */
public enum CycleTypeEnum {
    /**
     * 计划执行类型，0.定时一次执行、1.周期性每天执行 2.周期性每周执行
     */
    DELAY(0), DAY(1), WEEK(2);
    private final int type;

    CycleTypeEnum(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
