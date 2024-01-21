package io.bhex.broker.server.message;


/**
 * @author wangshouchao
 */
public enum NotifyType {
    /**
     * sub订阅，unsub部分取消订阅，cancel全部取消
     */
    PING(1), PONG(2), SUB(3), UNSUB(4), CANCEL(5), OK(9), ERROR(10);

    public final int code;

    NotifyType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
