/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_app_push_device")
public class AppPushDevice {

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
