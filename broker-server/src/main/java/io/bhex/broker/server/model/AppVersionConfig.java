/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import io.bhex.broker.server.domain.AppChannel;
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
@Table(name = "tb_app_version_config")
public class AppVersionConfig {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String appId;
    private String appVersion;
    private String deviceType;
    private String deviceVersion;
    private String appChannel;
    private Integer needUpdate;
    private Integer needForceUpdate;

    private String updateVersion;

    @Deprecated
    private String downloadUrl;

    private Long created;

    private Long updated;

    //此条配置是否被应用
    private Integer enableStatus;



//    public String getUniqueKey() {
//        return this.getOrgId() + "_" + this.getAppId() + "_" + this.getAppVersion() + "_" + this.getDeviceType() + "_" + this.getAppChannel();
//    }

    public String getUpdateVersionUniqueKey() {
        String channel = this.appChannel;
        if (this.appChannel.equals(AppChannel.GOOGLEPLAY.value())) {
            channel = AppChannel.OFFICIAL.value();
        }
        if (this.appChannel.equals(AppChannel.TESTFLIGHT.value()) || this.appChannel.equals(AppChannel.APPSTORE.value())) {
            channel = AppChannel.ENTERPRISE.value();
        }
        return this.getOrgId() + "_" + this.getAppId() + "_" + this.getUpdateVersion() + "_" + this.getDeviceType() + "_" + channel;
    }

}
