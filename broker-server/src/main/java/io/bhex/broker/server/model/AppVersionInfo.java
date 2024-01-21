/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/12/21
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
@Table(name = "tb_app_version_info")
public class AppVersionInfo {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String appId;
    private String appVersion;
    private String deviceType;
    private String deviceVersion;
    private String appChannel;
    private String downloadUrl;
    private String newFeatures;
    //private String guideImages;
    private Integer isLatestVersion;

    private String downloadWebviewUrl;

    private String googlePlayDownloadUrl;

    private String appStoreDownloadUrl;

    private String testflightDownloadUrl;

    private Long created;
    private Long updated;

    public String getUniqueKey() {
        return this.getOrgId() + "_" + this.getAppId() + "_" + this.getAppVersion() + "_" + this.getDeviceType() + "_" + this.getAppChannel();
    }

}
