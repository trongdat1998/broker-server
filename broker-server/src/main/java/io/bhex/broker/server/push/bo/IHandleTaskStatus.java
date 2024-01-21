package io.bhex.broker.server.push.bo;

import io.bhex.broker.server.model.AdminPushTask;

/**
 * 处理任务状态
 */
@FunctionalInterface
public interface IHandleTaskStatus {


    /**
     * 状态处理
     * @param adminPushTask 推送任务
     * @param taskRound 推送轮次（普通任务是1，周期任务是理论的执行次数）
     */
    void handle(AdminPushTask adminPushTask, Long taskRound);
}
