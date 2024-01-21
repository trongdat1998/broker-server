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
@Table(name = "tb_app_version_config_log")
public class AppVersionConfigLog {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private String appChannel;

    private String updateVersion;

    private String minVersion;

    private String maxVersion;

    private String deviceType;


    private Integer updateType; //0-可选  1-强制升级

    private String newFeatures;

    private Long adminUserId;

    private Long created;

    private Long updated;

    private Long groupId;

}
