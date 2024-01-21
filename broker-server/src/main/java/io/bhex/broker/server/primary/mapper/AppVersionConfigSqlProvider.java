/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import com.google.common.base.Strings;
import io.bhex.broker.server.model.AppVersionConfig;
import org.apache.ibatis.jdbc.SQL;


public class AppVersionConfigSqlProvider {

    public static final String TABLE_NAME = "tb_app_version_config";

    public static final String COLUMNS = "id, org_id, app_id, app_version, device_type, device_version, app_channel, "
            + "need_update, need_force_update, update_version, download_url, created, updated";

    public String getByAppVersion(AppVersionConfig versionConfig) {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}");
                if (!Strings.isNullOrEmpty(versionConfig.getAppId())) {
                    WHERE("app_id = #{appId}");
                }
                WHERE("app_version = #{appVersion}");
                WHERE("device_type = #{deviceType}");
                if (!Strings.isNullOrEmpty(versionConfig.getAppChannel())) {
                    WHERE("app_channel = #{appChannel}");
                }
            }
        }.toString();
    }

    public String queryAll() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
            }
        }.toString();
    }

}
