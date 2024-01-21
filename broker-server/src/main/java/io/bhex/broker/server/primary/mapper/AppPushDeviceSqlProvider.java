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
import io.bhex.broker.server.model.AppPushDevice;
import org.apache.ibatis.jdbc.SQL;

import java.util.List;
import java.util.StringJoiner;

public class AppPushDeviceSqlProvider {

    public static final String TABLE_NAME = "tb_app_push_device";

    public static final String COLUMNS = "id, org_id, user_id, app_id, app_version, device_type, device_version, app_channel, "
            + "device_token, client_id, third_push_type, push_limit, language, created, updated";

    public String insert(AppPushDevice pushDevice) {
        return new SQL() {
            {
                INSERT_INTO(TABLE_NAME);
                VALUES("org_id", "#{orgId}");
                VALUES("user_id", "#{userId}");
                if (!Strings.isNullOrEmpty(pushDevice.getAppId())) {
                    VALUES("app_id", "#{appId}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getAppVersion())) {
                    VALUES("app_version", "#{appVersion}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getDeviceType())) {
                    VALUES("device_type", "#{deviceType}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getDeviceVersion())) {
                    VALUES("device_version", "#{deviceVersion}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getAppChannel())) {
                    VALUES("app_channel", "#{appChannel}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getDeviceToken())) {
                    VALUES("device_token", "#{deviceToken}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getClientId())) {
                    VALUES("client_id", "#{clientId}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getThirdPushType())) {
                    VALUES("third_push_type", "#{thirdPushType}");
                }
                VALUES("push_limit", "#{pushLimit}");
                VALUES("language", "#{language}");
                VALUES("created", "#{created}");
                VALUES("updated", "#{updated}");
            }
        }.toString();
    }

    public String update(AppPushDevice pushDevice) {
        return new SQL() {
            {
                UPDATE(TABLE_NAME);
                if (pushDevice.getUserId() > 0) {
                    //只有userId有值既登录状态才更新updated
                    SET("user_id = #{userId}");
                    SET("updated = #{updated}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getAppId())) {
                    SET("app_id = #{appId}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getAppVersion())) {
                    SET("app_version = #{appVersion}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getDeviceType())) {
                    SET("device_type = #{deviceType}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getDeviceVersion())) {
                    SET("device_version = #{deviceVersion}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getAppChannel())) {
                    SET("app_channel = #{appChannel}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getDeviceToken())) {
                    SET("device_token = #{deviceToken}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getClientId())) {
                    SET("client_id = #{clientId}");
                }
                if (!Strings.isNullOrEmpty(pushDevice.getThirdPushType())) {
                    SET("third_push_type = #{thirdPushType}");
                }
                SET("push_limit = #{pushLimit}");
                SET("language = #{language}");
                WHERE("id = #{id}");
            }
        }.toString();
    }

    public String delete() {
        return new SQL() {
            {
                DELETE_FROM(TABLE_NAME).WHERE("id = #{id}");
            }
        }.toString();
    }

    public String query(Long id) {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("id = #{id}");
            }
        }.toString();
    }

    public String queryByUserId(AppPushDevice pushDevice) {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("user_id = #{userId}");
                ORDER_BY("updated desc");
            }
        }.toString();
    }

    public String queryByUserIds(Long orgId, List<Long> userIds) {
        StringJoiner stringJoiner = new StringJoiner(",");
        for (int i = 0; i < userIds.size(); i++) {
            //避免userId=0的数据查询到大量的未登录设备
            if (userIds.get(i) > 0) {
                stringJoiner.add(String.valueOf(userIds.get(i)));
            }
        }
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = " + orgId + " and user_id in (" + stringJoiner.toString() + ")");
                ORDER_BY("id asc");
            }
        }.toString();
    }

    public String queryByUserIdForAddDevice(AppPushDevice pushDevice) {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("user_id = #{userId}");
                WHERE("device_type = #{deviceType}");
                WHERE("third_push_type = #{thirdPushType}");
            }
        }.toString();
    }

    public String queryByDeviceToken(AppPushDevice pushDevice) {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("device_type = #{deviceType}", "third_push_type = #{thirdPushType}", "device_token = #{deviceToken}");
            }
        }.toString();
    }
}
