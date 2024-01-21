package io.bhex.broker.server.domain.staking;

/**
 * 理财staking矿池派息周期
 */
public enum StakingPoolRebateCycle {

    /**
     * 日
     */
    DAILY(0),

    /**
     * 周
     */
    WEEKLY(1),

    /**
     * 月
     */
    MONTHLY(2);

    private int cycle;

    StakingPoolRebateCycle(int type) {
        this.cycle = cycle;
    }

    public int getCycle() {
        return cycle;
    }

    public static StakingPoolRebateCycle fromValue(int cycle) {
        for (StakingPoolRebateCycle s : StakingPoolRebateCycle.values()) {
            if (s.getCycle() == cycle) {
                return s;
            }
        }
        return null;
    }
}
