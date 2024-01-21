package io.bhex.broker.server.domain;

public enum ConvertOrderStatus {

    // 兑换订单状态 1未支付，2已支付，3订单失败
    UNPAID(1),
    PAID(2),
    FAILED(3);

    private int status;

    ConvertOrderStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static ConvertOrderStatus fromStatus(int status) {
        for (ConvertOrderStatus at : ConvertOrderStatus.values()) {
            if (at.getStatus() == status) {
                return at;
            }
        }
        return null;
    }

}
