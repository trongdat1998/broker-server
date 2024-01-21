package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 04/09/2018 6:10 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class AnnouncementSqlProvider {

    public String count(Map<String, Object> parameter) {

        Integer platform = (Integer) parameter.get("platform");
        return new SQL() {
            {
                SELECT("count(*)").FROM(AnnouncementMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("status=1");
                if (platform != null && platform != 0) {
                    WHERE("platform = #{platform}");
                }
            }
        }.toString();
    }

    public String queryAnnouncement(Map<String, Object> parameter) {

        Integer platform = (Integer) parameter.get("platform");
        return new SQL() {
            {
                SELECT(AnnouncementMapper.COLUMNS).FROM(AnnouncementMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("status=1");
                if (platform != null && platform != 0) {
                    WHERE("platform = #{platform}");
                }
                ORDER_BY("id DESC limit #{fromindex}, #{endindex}");
            }
        }.toString();
    }

}
