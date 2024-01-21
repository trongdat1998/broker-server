package io.bhex.broker.server.statistics.clear.mapper;


import java.util.List;
import java.util.Map;

public class StatisticsSqlProvider {

    public String queryComeInBalanceFlow(Map<String, Object> parameter) {
        final List<Long> accountId = (List<Long>) parameter.get("accountId");
        StringBuilder sqlBuilder = new StringBuilder("select account_id, sum(changed) as changed from ods_balance_flow use index (idx_accountid_businesssubject) where business_subject in (3,51)");
        sqlBuilder.append(" and token_id=#{tokenId}");
        sqlBuilder.append(" and created_at between #{startTime} and #{endTime}");
        sqlBuilder.append(" and changed > 0");
        sqlBuilder.append(" and account_id in(");
        for (int i = 0; i < accountId.size(); i++) {
            sqlBuilder.append(String.format("#{accountId[%d]}", i));
            if (i < accountId.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        sqlBuilder.append(" group by account_id");
        return sqlBuilder.toString();
    }

    public String queryOutgoBalanceFlow(Map<String, Object> parameter) {
        final List<Long> accountId = (List<Long>) parameter.get("accountId");
        StringBuilder sqlBuilder = new StringBuilder("select account_id, sum(changed) as changed from ods_balance_flow use index (idx_accountid_businesssubject) where business_subject in (3,51)");
        sqlBuilder.append(" and token_id=#{tokenId}");
        sqlBuilder.append(" and created_at between #{startTime} and #{endTime}");
        sqlBuilder.append(" and changed < 0");
        sqlBuilder.append(" and account_id in(");
        for (int i = 0; i < accountId.size(); i++) {
            sqlBuilder.append(String.format("#{accountId[%d]}", i));
            if (i < accountId.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        sqlBuilder.append(" group by account_id");
        return sqlBuilder.toString();
    }

    public String queryAccountBalanceFlow(Map<String, Object> parameter) {
        final Long accountId = (Long) parameter.get("accountId");
        final Integer limit = (Integer) parameter.get("limit");
        final Long lastId = (Long) parameter.get("lastId");
        final List<Integer> businessSubject = (List<Integer>) parameter.get("businessSubject");
        final String tokenId = (String) parameter.get("tokenId");

        StringBuilder sqlBuilder = new StringBuilder("select id, account_id, changed,token_id, total,created_at,business_subject from ods_balance_flow use index (idx_accountid_businesssubject)");
        sqlBuilder.append("  where account_id  = ").append(accountId);
        if (lastId != null && lastId > 0) {
            sqlBuilder.append(" and id < ").append(lastId);
        }
        if (businessSubject != null && businessSubject.size() > 0) {
            sqlBuilder.append(" and business_subject in(");
            for (int i = 0; i < businessSubject.size(); i++) {
                sqlBuilder.append(String.format("#{businessSubject[%d]}", i));
                if (i < businessSubject.size() - 1) {
                    sqlBuilder.append(",");
                }
            }
            sqlBuilder.append(")");
        }
        if (tokenId != null && !tokenId.equals("")) {
            sqlBuilder.append(" and token_id=#{tokenId}");
        }
        sqlBuilder.append(" order by id desc");
        sqlBuilder.append(" limit ").append(limit);

        return sqlBuilder.toString();
    }
}
