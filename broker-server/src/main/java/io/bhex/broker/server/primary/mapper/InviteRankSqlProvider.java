package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

public class InviteRankSqlProvider {

    private static final String TABLE_NAME = "tb_invite_rank";

    private static final String COLUMNS = "id, user_id,user_name,month,type,amount,status,created_at,updated_at";


    public String getInviteRankByTypeAndMonth() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE(" month = #{month} ");
                WHERE(" type = #{type} ");
                WHERE(" status = 0 ");
            }
        }.toString();
    }


    public String buildInviteRank() {
        return new SQL() {
            {
                SELECT(" user_id ,sum(bonus_coin) amount");
                FROM("tb_invite_bonus_record");
                WHERE(" statistics_time >= #{time}");
                WHERE(" act_type = #{actType}");
                GROUP_BY(" user_id");
                ORDER_BY(" amount desc limit 50");
            }
        }.toString();
    }

    public String getInviteRankMonthList() {
        return new SQL() {
            {
                SELECT("DISTINCT(month) month")
                        .FROM(TABLE_NAME)
                        .ORDER_BY(" month desc limit 10");
            }
        }.toString();
    }

}
