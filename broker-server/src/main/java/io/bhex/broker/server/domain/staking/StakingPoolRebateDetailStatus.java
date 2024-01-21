package io.bhex.broker.server.domain.staking;

/**
 * 理财staking矿池派息明细状态
 */
public enum StakingPoolRebateDetailStatus {

    /**
     * 待转账
     */
    WAITING(0),

    /**
     * 已转账
     */
    SUCCESS(1),

    /**
     * 已取消
     */
    CANCELED(2);

    private int status;

    StakingPoolRebateDetailStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static StakingPoolRebateDetailStatus fromValue(int status) {
        for (StakingPoolRebateDetailStatus s : StakingPoolRebateDetailStatus.values()) {
            if (s.getStatus() == status) {
                return s;
            }
        }
        return null;
    }
}
