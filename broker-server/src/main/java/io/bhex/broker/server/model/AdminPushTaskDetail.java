package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 等同于流水
 * author: wangshouchao
 * Date: 2020/07/25 06:23:37
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_admin_push_task_detail")
public class AdminPushTaskDetail {

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
     * 推送次数,单次是1,循环叠加
     */
    private Long taskRound;
    /**
     * 应用程序id
     */
    private String appId;
    /**
     * app通道
     */
    private String appChannel;
    /**
     * 三方通道
     */
    private String thirdPushType;
    /**
     * 设备类型
     */
    private String deviceType;
    /**
     * 国际化语言
     */
    private String language;
    /**
     * 实际执行时间
     */
    private Long actionTime;
    /**
     * 请求common-server时用的id，用于回调信息时匹配
     */
    private String reqOrderId;
    /**
     * 本次分组的开始id
     */
    private Long startId;
    /**
     * 本次分组的结束id
     */
    private Long endId;
    /**
     * 发送数量
     */
    private Integer sendCount;
    /**
     * 触达数量
     */
    private Long deliveryCount;
    /**
     * 链接点击数量
     */
    private Long clickCount;
    /**
     * 备注成功与否
     */
    private String remark;
    /**
     * created
     */
    private Long created;
    /**
     * updated
     */
    private Long updated;
}