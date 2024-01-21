package io.bhex.broker.server.push.bo;

import lombok.Data;

/**
 * @author wangsc
 * @description redis缓存(唯一性taskId + taskRound)
 * @date 2020-07-27 18:55
 */
@Data
public class AppPushTaskCache {
    /**
     * 任务id
     */
    private Long taskId;
    /**
     * broker id（用于获取机构下设备分组）
     */
    private Long orgId;
    /**
     * 推送循环次数
     */
    private Long taskRound;
    /**
     * 执行状态(0-未执行 1-执行中 2-此轮任务执行完成  3-此task结束  4-取消)
     */
    private Integer status;
    /**
     * 更新时间戳,正在执行中的时间戳,两分钟依然不更新至结束的，考虑开始补偿确认
     */
    private Long updated;
    /**
     * 重试次数(允许重试三次或五次)
     */
    private Integer retryNum;
}
