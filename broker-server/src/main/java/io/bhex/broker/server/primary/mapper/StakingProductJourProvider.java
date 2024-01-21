package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

import java.util.Map;


public class StakingProductJourProvider {
    public static final String TABLE_NAME = "tb_staking_product_jour";

    public String getProductJourList(Map<String, Object> params) {

        SQL sql = new SQL();
        sql.SELECT("*").FROM(TABLE_NAME);

        sql.WHERE("org_id = #{orgId}");
        sql.WHERE("user_id = #{userId}");

        if ((long) params.get("productId") != 0) {
            sql.WHERE("product_id = #{productId}");
        }

        if ((int) params.get("productType") != -1) {
            sql.WHERE("product_type = #{productType}");
        } else {
            sql.WHERE("product_type != 1");
        }

        if ((int) params.get("type") != -1) {
            sql.WHERE("type = #{type}");
        }

        //派息记录或者是活期的申购赎回、派息记录
        //sql.WHERE("(type = 1 or product_type = 1)");
        //先不查询活期的数据


        if ((long) params.get("jourId") > 0) {
            sql.WHERE("id = #{jourId}");
        } else if ((long) params.get("startId") > 0) {
            sql.WHERE("id < #{startId}");
        }

        sql.ORDER_BY("id desc limit #{limit}");

        return sql.toString();
    }

}
