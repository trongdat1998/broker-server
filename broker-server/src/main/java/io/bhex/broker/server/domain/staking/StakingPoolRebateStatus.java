package io.bhex.broker.server.domain.staking;

/**
 * 理财staking矿池派息状态
 */
public enum StakingPoolRebateStatus {

    /**
     * 待计算
     */
    WAITING(0),

    /**
     * 已计算
     */
    CALCED(1),

    /**
     * 已派息
     */
    SUCCESS(2),

    /**
     * 已取消
     */
    CANCELED(3);

    private int status;

    StakingPoolRebateStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static StakingPoolRebateStatus fromValue(int status) {
        for (StakingPoolRebateStatus s : StakingPoolRebateStatus.values()) {
            if (s.getStatus() == status) {
                return s;
            }
        }
        return null;
    }
}
