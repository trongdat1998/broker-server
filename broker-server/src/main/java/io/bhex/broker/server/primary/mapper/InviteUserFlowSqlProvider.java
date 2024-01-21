package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

public class InviteUserFlowSqlProvider {

    public static final String TABLE_NAME = "tb_invite_user_fee_detail";

    public String getInviteUserFlowUserAccountId() {
        return new SQL() {
            {
                SELECT("DISTINCT(account_id)").FROM(TABLE_NAME);
                WHERE(" org_id = #{orgId}");
                WHERE(" type = #{type}");
                WHERE("statistics_time = #{time}");
                ORDER_BY("account_id asc limit #{start},#{limit}");
            }
        }.toString();
    }

}
