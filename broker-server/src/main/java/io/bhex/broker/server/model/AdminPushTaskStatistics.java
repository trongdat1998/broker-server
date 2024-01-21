package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * author: wangshouchao
 * Date: 2020/07/25 06:25:33
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_admin_push_task_statistics")
public class AdminPushTaskStatistics implements Serializable {

    /**
     * auto increase
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * task id
     */
    private Long taskId;
    /**
     * broker id
     */
    private Long orgId;
    /**
     * 推送循环次数
     */
    private Long taskRound;
    /**
     * 发送数量
     */
    private Long sendCount;
    /**
     * 触达数量
     */
    private Long deliveryCount;
    /**
     * 链接点击数量
     */
    private Long clickCount;
    /**
     * 有效数量
     */
    private Long effectiveCount;
    /**
     * 退订数量
     */
    private Long unsubscribeCount;
    /**
     * 卸载数量
     */
    private Long uninstallCount;
    /**
     * 取消推送权限数量
     */
    private Long cancelCount;
    /**
     * created
     */
    private Long created;
    /**
     * updated
     */
    private Long updated;
}