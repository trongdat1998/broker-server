package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

public class ActivityLockInterestProvider {

    public String orgListActivityByPage(Map<String, Object> parameter) {
        Object fromId = parameter.get("fromId");
        Object endId = parameter.get("endId");
        Object orderDesc = parameter.get("orderDesc");
        return new SQL() {
            {
                SELECT(ActivityLockInterestMapper.COLUMNS).FROM(ActivityLockInterestMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("status > 0 ");
                if (fromId != null && (Long) fromId > 0) {
                    WHERE("id < #{fromId}");
                }
                if (endId != null && (Long) endId != 0) {
                    WHERE("id > #{endId}");
                }

                if (orderDesc != null && (Boolean) orderDesc){
                    ORDER_BY("id DESC limit #{size}");
                }else {
                    ORDER_BY("id limit #{size}");
                }

            }
        }.toString();
    }
}
