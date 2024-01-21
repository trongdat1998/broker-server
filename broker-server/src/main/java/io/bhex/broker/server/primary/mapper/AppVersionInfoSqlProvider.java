/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/12/21
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

public class AppVersionInfoSqlProvider {

    private static final String COLUMNS = "id, org_id, app_id, app_version, device_type, device_version, app_channel, download_url, new_features, is_latest_version, download_webview_url, created, updated";

    private static final String TABLE_NAME = "tb_app_version_info";

    public String queryAll() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
            }
        }.toString();
    }

}
