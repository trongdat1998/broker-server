package io.bhex.broker.server.domain;

public enum OtcThirdPartyOrderStatus {

    PENDING_PAYMENT(1), // 待付款
    WAITING_PAYMENT(2), // 已付款，待确认
    IN_PROGRESS(3), // 放币处理中
    COIN_TRANSFERRED(4), // 放币完成
    EXPIRED(5), // 订单超时
    DECLINED(6), // 订单拒绝
    CANCELLED(7); // 订单取消

    private int status;

    OtcThirdPartyOrderStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

}

