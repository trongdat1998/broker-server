package io.bhex.broker.server.primary.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.primary.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/8/4 4:21 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
public class ActivityOrderProvider {

    public String queryAllOrderByPage(Map<String, Object> parameter) {
        Object fromId = parameter.get("fromId");
        Object endId = parameter.get("endId");
        return new SQL() {
            {
                SELECT(ActivityLockInterestOrderMapper.COLUMNS).FROM(ActivityLockInterestOrderMapper.TABLE_NAME);
                WHERE("account_id = #{accountId}");
                WHERE("broker_id = #{brokerId}");
                WHERE("status = 1");
                if (fromId != null && (Long) fromId > 0) {
                    WHERE("id < #{fromId}");
                }
                if (endId != null && (Long) endId != 0) {
                    WHERE("id > #{endId}");
                }
                ORDER_BY("id DESC limit #{limit}");
            }
        }.toString();
    }

    public String orgQueryOrderByPage(Map<String, Object> parameter) {
        Object fromId = parameter.get("fromId");
        Object endId = parameter.get("endId");
        Object orderDesc = parameter.get("orderDesc");

        return new SQL() {
            {
                SELECT(ActivityLockInterestOrderMapper.COLUMNS).FROM(ActivityLockInterestOrderMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("project_code = #{projectCode}");
                WHERE("status = 1");
                if (fromId != null && (Long) fromId > 0) {
                    WHERE("id < #{fromId}");
                }
                if (endId != null && (Long) endId != 0) {
                    WHERE("id > #{endId}");
                }
                if (orderDesc != null && (Boolean) orderDesc) {
                    ORDER_BY("id DESC limit #{limit}");
                } else {
                    ORDER_BY("id limit #{limit}");
                }
            }
        }.toString();
    }

    public String orgQueryUserOrderByPage(Map<String, Object> parameter) {
        Object fromId = parameter.get("fromId");
        Object endId = parameter.get("endId");
        Object orderDesc = parameter.get("orderDesc");

        return new SQL() {
            {
                SELECT(ActivityLockInterestOrderMapper.COLUMNS).FROM(ActivityLockInterestOrderMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("user_id = #{userId}");
                WHERE("status = 1");
                if (fromId != null && (Long) fromId > 0) {
                    WHERE("id < #{fromId}");
                }
                if (endId != null && (Long) endId != 0) {
                    WHERE("id > #{endId}");
                }
                if (orderDesc != null && (Boolean) orderDesc) {
                    ORDER_BY("id DESC limit #{limit}");
                } else {
                    ORDER_BY("id limit #{limit}");
                }
            }
        }.toString();
    }


    public String queryAllOrderByPageByUidAndProjectId(Map<String, Object> parameter) {
        Object fromId = parameter.get("fromId");
        Object endId = parameter.get("endId");
        Object projectId = parameter.get("projectId");
        Object userId = parameter.get("userId");
        String projectCode = (String) parameter.get("projectCode");

        return new SQL() {
            {
                SELECT(ActivityLockInterestOrderMapper.COLUMNS).FROM(ActivityLockInterestOrderMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("status = 1");
                if (userId != null && (Long) userId > 0) {
                    WHERE("user_id = #{userId}");
                }
                if (projectId != null && (Long) projectId > 0) {
                    WHERE("project_id = #{projectId}");
                }
                if (StringUtils.isNotEmpty(projectCode)) {
                    WHERE("project_code = #{projectCode}");
                }
                if (fromId != null && (Long) fromId > 0) {
                    WHERE("id < #{fromId}");
                }
                if (endId != null && (Long) endId != 0) {
                    WHERE("id > #{endId}");
                }
                ORDER_BY("id DESC limit #{limit}");
            }
        }.toString();
    }
}
