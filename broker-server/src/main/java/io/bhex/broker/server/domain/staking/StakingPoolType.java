package io.bhex.broker.server.domain.staking;

/**
 * 理财staking矿池类型
 */
public enum StakingPoolType {

    /**
     * 持币生息
     */
    HOLD(1),

    /**
     * 锁仓
     */
    LOCK(2);

    private int type;

    StakingPoolType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static StakingPoolType fromValue(int type) {
        for (StakingPoolType s : StakingPoolType.values()) {
            if (s.getType() == type) {
                return s;
            }
        }
        return null;
    }
}
