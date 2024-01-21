package io.bhex.broker.server.primary.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

/**
 * @Description:
 * @Date: 2018/9/19 下午5:08
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
public class WithdrawOrderSqlProvider {

    public String queryOrders(Map<String, Object> parameter) {

        Long fromOrderId = (Long) parameter.get("fromOrderId");
        Long endOrderId = (Long) parameter.get("endOrderId");
        Long accountId = (Long) parameter.get("accountId");
        String tokenId = (String) parameter.get("tokenId");
        Integer status = (Integer) parameter.get("status");
        return new SQL() {
            {
                SELECT("*");
                FROM("tb_withdraw_order");
                WHERE("org_id = #{orgId}");
                if (accountId != null && accountId > 0L) {
                    WHERE("account_id = #{accountId}");
                }
                if (!StringUtils.isEmpty(tokenId)) {
                    WHERE("token_id = #{tokenId}");
                }
                if (fromOrderId != null && fromOrderId > 0L) {
                    WHERE("id < #{fromOrderId}");
                }
                if (endOrderId != null && endOrderId > 0L) {
                    WHERE("id > #{endOrderId}");
                }
                if (status != null && status > 0L) {
                    WHERE("status = #{status}");
                }
                ORDER_BY("id desc");

            }
        }.toString() + " LIMIT #{limit}";
    }

}
