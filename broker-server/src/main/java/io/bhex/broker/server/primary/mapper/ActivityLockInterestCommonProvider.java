package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

public class ActivityLockInterestCommonProvider {

    public String listActivityByPage(Map<String, Object> parameter) {
        Object fromId = parameter.get("fromId");
        Object endId = parameter.get("endId");

        return new SQL() {
            {
                SELECT(ActivityLockInterestCommonMapper.LIST_COLUMNS).FROM(ActivityLockInterestCommonMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("language = #{language}");
                WHERE("activity_type in (2,3)");
                WHERE("status > 0 ");
                if (fromId != null && (Long) fromId > 0) {
                    WHERE("id < #{fromId}");
                }
                if (endId != null && (Long) endId != 0) {
                    WHERE("id > #{endId}");
                }

                ORDER_BY("id DESC limit #{size}");
            }
        }.toString();
    }
}
