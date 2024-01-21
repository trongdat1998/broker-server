package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * author: wangshouchao
 * Date: 2020/07/25 06:26:29
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_app_push_device_history")
public class AppPushDeviceHistory {

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
     * user id
     */
    private Long userId;
    /**
     * app id
     */
    private String appId;
    /**
     * app version
     */
    private String appVersion;
    /**
     * device type, like: ios android mac windows
     */
    private String deviceType;
    /**
     * device version
     */
    private String deviceVersion;
    /**
     * app channel
     */
    private String appChannel;
    /**
     * device token
     */
    private String deviceToken;
    /**
     * client id
     */
    private String clientId;
    /**
     * third push type, like Umeng, getui
     */
    private String thirdPushType;
    /**
     * 1开、0未开
     */
    private Integer pushLimit;
    /**
     * 上次设置的语言
     */
    private String language;
    /**
     * created
     */
    private Long created;
    /**
     * updated
     */
    private Long updated;
}