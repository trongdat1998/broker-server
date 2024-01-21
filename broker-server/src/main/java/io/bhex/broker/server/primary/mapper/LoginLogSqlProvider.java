/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.service.user
 *@Date 2018/8/3
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

public class LoginLogSqlProvider {

    private static final String TABLE_NAME = "tb_login_log";
    private static final String COLUMNS = "id, user_id, ip, region, login_type, status, platform, user_agent, language, created, updated, app_base_header";

    public String update() {
        return "UPDATE " + TABLE_NAME + " SET status = #{status} WHERE id = #{id}";
    }

    public String queryLog(Map<String, Object> parameter) {
        Object fromId = parameter.get("fromId");
        Object startTime = parameter.get("startTime");
        Object endTime = parameter.get("endTime");
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("user_id = #{userId}");
                if (fromId != null && (Long) fromId > 0) {
                    WHERE("id < #{fromId}");
                }
                if (startTime != null && (Long) startTime != 0) {
                    WHERE("created > #{startTime}");
                }
                if (endTime != null && (Long) endTime != 0) {
                    WHERE("created <= #{endTime}");
                }
                ORDER_BY("id DESC limit #{limit}");
            }
        }.toString();
    }

    public String getLogs(Map<String, Object> parameter) {
        Object startTimeObj = parameter.get("startTime");
        Object endTimeObj = parameter.get("endTime");
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("user_id = #{userId}");
                if (startTimeObj != null && (Long) startTimeObj > 0) {
                    WHERE("created > #{startTime}");
                }
                if (endTimeObj != null && (Long) endTimeObj > 0) {
                    WHERE("created <= #{endTime}");
                }
                ORDER_BY("id DESC limit #{start},#{offset}");
            }
        }.toString();
    }

    public String countLogs(Map<String, Object> parameter) {
        Object startTimeObj = parameter.get("startTime");
        Object endTimeObj = parameter.get("endTime");
        return new SQL() {
            {
                SELECT("count(*)").FROM(TABLE_NAME);
                WHERE("user_id = #{userId}");
                if (startTimeObj != null && (Long) startTimeObj > 0) {
                    WHERE("created > #{startTime}");
                }
                if (endTimeObj != null && (Long) endTimeObj > 0) {
                    WHERE("created <= #{endTime}");
                }
            }
        }.toString();
    }

    public String getById() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME).WHERE("id = #{id}");
            }
        }.toString();
    }

    public String getLastLoginLog() {
        return "SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE user_id=#{userId} AND platform=#{platform} ORDER BY id desc LIMIT 1";
//        return new SQL() {
//            {
//                SELECT(COLUMNS).FROM(TABLE_NAME);
//                WHERE("user_id = #{userId}");
//                ORDER_BY("id DESC limit 1");
//            }
//        }.toString();
    }

}
