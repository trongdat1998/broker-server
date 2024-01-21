package io.bhex.broker.server.push.bo;

/**
 * 推送任务状态
 */
public enum PushTaskStatusEnum {
    /**
     * 非周期 INITIAL->PUSHING->FINISH
     * 周期  INITIAL->PUSHING-PERIOD_FINISH->PUSHING->PERIOD_FINISH ... ->FINISH
     */
    INITIAL(0),PUSHING(1),PERIOD_FINISH(2),FINISH(3),CANCEL(4);
    private final int status;
    PushTaskStatusEnum(int status){
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
