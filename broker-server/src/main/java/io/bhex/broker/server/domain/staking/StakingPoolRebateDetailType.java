package io.bhex.broker.server.domain.staking;

/**
 * 理财staking矿池派息记录类型
 */
public enum StakingPoolRebateDetailType {

    /**
     * 利息
     */
    INTEREST(0),

    /**
     * 本金
     */
    PRINCIPAL(1);

    private int type;

    StakingPoolRebateDetailType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static StakingPoolRebateDetailType fromValue(int type) {
        for (StakingPoolRebateDetailType s : StakingPoolRebateDetailType.values()) {
            if (s.getType() == type) {
                return s;
            }
        }
        return null;
    }
}
