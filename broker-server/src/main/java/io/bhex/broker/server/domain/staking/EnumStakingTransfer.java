package io.bhex.broker.server.domain.staking;

/**
 * 转账状态
 *
 * @author bhex
 * @date
 */
public enum EnumStakingTransfer {
    /**
     * 待确认
     */
    UNKNOWN(100),

    /**
     * 成功
     */
    SUCCESS(200),

    /**
     * 失败
     */
    FAIL(300);


    private int status;

    EnumStakingTransfer(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static EnumStakingTransfer fromValue(int status) {
        for (EnumStakingTransfer s : EnumStakingTransfer.values()) {
            if (s.getStatus() == status) {
                return s;
            }
        }
        return null;
    }
}
