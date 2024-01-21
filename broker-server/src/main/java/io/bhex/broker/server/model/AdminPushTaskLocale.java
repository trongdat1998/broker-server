package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 推送任务国际化内容表
 * author: wangshouchao
 * Date: 2020/07/25 06:25:01
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_admin_push_task_locale")
public class AdminPushTaskLocale {

    /**
     * auto increase
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * broker id
     */
    private Long orgId;
    /**
     * task id
     */
    private Long taskId;
    /**
     * 语言
     */
    private String language;
    /**
     * 推送概要
     */
    private String pushSummary;
    /**
     * 推送主题
     */
    private String pushTitle;
    /**
     * 推送内容
     */
    private String pushContent;
    /**
     * 推送url
     */
    private String pushUrl;
    /**
     * url类型
     */
    private Integer urlType;
    /**
     * created
     */
    private Long created;
    /**
     * updated
     */
    private Long updated;
}