package io.bhex.broker.server.domain.staking;

/**
 * 理财staking矿池派息计算方式
 */
public enum StakingPoolCalcInterestType {

    /**
     * 按年化利率
     */
    YEAR_RATE(0),

    /**
     * 按固定额度
     */
    FIXED_AMOUNT(1);

    private int type;

    StakingPoolCalcInterestType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static StakingPoolCalcInterestType fromValue(int type) {
        for (StakingPoolCalcInterestType s : StakingPoolCalcInterestType.values()) {
            if (s.getType() == type) {
                return s;
            }
        }
        return null;
    }
}
