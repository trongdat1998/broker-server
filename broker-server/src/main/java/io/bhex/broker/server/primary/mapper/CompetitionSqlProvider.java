package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.TradeCompetitionResult;

import java.util.List;
import java.util.Map;

public class CompetitionSqlProvider {

//    public String saveTradeCompetitionResult(Map<String, Object> parameter) {
//        final List<TradeCompetitionResult> list = (List<Long>) parameter.get("list");
//        StringBuilder sqlBuilder = new StringBuilder("select account_id, sum(changed) from shard_tb_balance_flow where business_subject = #{businessSubject}");
//        sqlBuilder.append(" and token_id=#{tokenId}");
//        sqlBuilder.append(" and created_at between #{startTime} and {endTime}");
//        sqlBuilder.append(" and changed > 0");
//        sqlBuilder.append(" and account_id in(");
//        for (int i = 0; i < list.size(); i++) {
//            sqlBuilder.append(String.format("#{accountId[%d]}", i));
//            if (i < accountId.size() - 1) {
//                sqlBuilder.append(",");
//            }
//        }
//        sqlBuilder.append(")");
//        sqlBuilder.append(" group by account_id");
//        return sqlBuilder.toString();
//    }
}
