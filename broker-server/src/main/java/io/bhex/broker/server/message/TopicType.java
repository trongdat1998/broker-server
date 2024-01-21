package io.bhex.broker.server.message;

/**
 * @author wangshouchao
 */
public enum TopicType {
    /**
     * cancel全部取消（3-5是可以消费的主题）
     */
    PING(1), PONG(2), CANCEL(6), OK(9), ERROR(10);

    public final int code;

    TopicType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
