package io.bhex.broker.server.primary.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

public class WithdrawOrderVerifyHistoryProvider {

    public String queryOrdersHistory(Map<String, Object> parameter) {
        Long bhWithdrawOrderId = (Long) parameter.get("bhWithdrawOrderId");
        Long fromOrderId = (Long) parameter.get("fromOrderId");
        Long endOrderId = (Long) parameter.get("endOrderId");
        Long accountId = (Long) parameter.get("accountId");
        String tokenId = (String) parameter.get("tokenId");
        Integer status = (Integer) parameter.get("status");
        StringBuilder sqlBuilder = new StringBuilder("select a.* from tb_withdraw_order_verify_history a ");
        if (bhWithdrawOrderId != null && bhWithdrawOrderId > 0L) {
            sqlBuilder.append(" JOIN tb_withdraw_order b ON a.withdraw_order_id=b.id ");
        }


        sqlBuilder.append("where a.org_id = #{orgId}");
        if (accountId != null && accountId > 0L) {
            sqlBuilder.append(" and a.account_id = #{accountId}");
        }
        if (bhWithdrawOrderId != null && bhWithdrawOrderId > 0L) {
            sqlBuilder.append(" and a.withdraw_order_id = b.id and b.order_id = #{bhWithdrawOrderId} ");
        }
//                if (!StringUtils.isEmpty(tokenId)) {
//                    WHERE("a.token_id = #{tokenId}");
//                }
        if (fromOrderId != null && fromOrderId > 0L) {
            sqlBuilder.append(" and a.id < #{fromOrderId}");
        }
        if (endOrderId != null && endOrderId > 0L) {
            sqlBuilder.append(" and a.id > #{endOrderId}");
        }
        if (status != null && status > 0L) {
            sqlBuilder.append(" and a.status = #{status}");
        }
        sqlBuilder.append(" order by a.id desc").append(" LIMIT #{limit}");
        return sqlBuilder.toString();
//        return new SQL() {
//            {
//                SELECT("a.*");
//                FROM("tb_withdraw_order_verify_history a");
//                //JOIN bh_tb_account b ON a.account_id=b.account_id
//                if (bhWithdrawOrderId != null && bhWithdrawOrderId > 0L) {
//                    WHERE("JOIN bh_tb_account b ON a.account_id=b.account_id");
//                }
//
//                WHERE("a.org_id = #{orgId}");
//
//
//                if (accountId != null && accountId > 0L) {
//                    WHERE("a.account_id = #{accountId}");
//                }
////                if (!StringUtils.isEmpty(tokenId)) {
////                    WHERE("a.token_id = #{tokenId}");
////                }
//                if (fromOrderId != null && fromOrderId > 0L) {
//                    WHERE("a.id < #{fromOrderId}");
//                }
//                if (endOrderId != null && endOrderId > 0L) {
//                    WHERE("a.id > #{endOrderId}");
//                }
//                if (status != null && status > 0L) {
//                    WHERE("a.status = #{status}");
//                }
//                ORDER_BY("a.id desc");
//
//            }
//        }.toString() + " LIMIT #{limit}";
    }


}
