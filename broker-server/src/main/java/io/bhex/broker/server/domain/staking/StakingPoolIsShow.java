package io.bhex.broker.server.domain.staking;

/**
 * 理财staking矿池状态
 */
public enum StakingPoolIsShow {

    /**
     * 下架
     */
    No(0),

    /**
     * 上架
     */
    Yes(1);

    private int status;

    StakingPoolIsShow(int status) {
        this.status = status;
    }

    public int getIsShow() {
        return status;
    }

    public static StakingPoolIsShow fromValue(int status) {
        for (StakingPoolIsShow s : StakingPoolIsShow.values()) {
            if (s.getIsShow() == status) {
                return s;
            }
        }
        return null;
    }
}
