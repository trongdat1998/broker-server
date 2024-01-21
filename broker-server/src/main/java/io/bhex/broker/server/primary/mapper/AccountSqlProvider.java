/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import com.google.common.base.Strings;
import io.bhex.broker.server.model.Account;
import org.apache.ibatis.jdbc.SQL;

import java.util.List;
import java.util.Map;

public class AccountSqlProvider {

    private static final String TABLE_NAME = "tb_account";

    private static final String COLUMNS = "id, org_id, user_id, account_id, account_name, account_type, account_index, account_status, created,is_forbid,forbid_start_time,forbid_end_time,is_authorized_org";

    public String getByAccountIdLock() {
        StringBuffer sql = new StringBuffer();
        sql.append(" select ");
        sql.append(COLUMNS);
        sql.append(" from ");
        sql.append(TABLE_NAME);
        sql.append(" where account_id = #{accountId} ");
        sql.append(" for update ");

        return sql.toString();
    }

    public String getByAccountId() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("account_id = #{accountId}", "account_status = 1").ORDER_BY("id desc");
            }
        }.toString();
    }

    public String getMainAccount() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}", "user_id = #{userId}", "account_type = 1", "account_index = 0");
            }
        }.toString();
    }

    public String getAccountByType() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}", "user_id = #{userId}", "account_type = #{accountType}", "account_index = #{accountIndex}");
            }
        }.toString();
    }

    public String insert(Account account) {
        return new SQL() {
            {
                INSERT_INTO(TABLE_NAME);
                if (account.getOrgId() != null) {
                    VALUES("org_id", "#{orgId}");
                }
                if (account.getUserId() != null) {
                    VALUES("user_id", "#{userId}");
                }
                if (account.getAccountId() != null) {
                    VALUES("account_id", "#{accountId}");
                }
                if (!Strings.isNullOrEmpty(account.getAccountName())) {
                    VALUES("account_name", "#{accountName}");
                }
                if (account.getAccountType() != null) {
                    VALUES("account_type", "#{accountType}");
                }
                if (account.getAccountIndex() != null) {
                    VALUES("account_index", "#{accountIndex}");
                }
                VALUES("created", "#{created}");
                VALUES("updated", "#{updated}");
            }
        }.toString();
    }

    public String update(Account account) {
        return new SQL() {
            {
                UPDATE(TABLE_NAME);
                if (!Strings.isNullOrEmpty(account.getAccountName())) {
                    SET("account_name = #{accountName}");
                }
                if (account.getAccountStatus() != null) {
                    SET("account_status = #{accountStatus}");
                }
                SET("updated = #{updated}");
                WHERE("account_id = #{accountId}");
            }
        }.toString();
    }

    public String queryFuturesAccountList(Map<String, Object> parameter) {
        final List<Long> userId = (List<Long>) parameter.get("userId");
        StringBuilder sqlBuilder = new StringBuilder("select account_id,user_id from tb_account");
        sqlBuilder.append(" where account_type = 3");
        sqlBuilder.append(" and user_id in(");
        for (int i = 0; i < userId.size(); i++) {
            sqlBuilder.append(String.format("#{userId[%d]}", i));
            if (i < userId.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

}
