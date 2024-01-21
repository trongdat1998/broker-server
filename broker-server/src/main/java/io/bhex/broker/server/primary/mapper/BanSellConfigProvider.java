package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;
import io.bhex.broker.server.model.BanSellConfig;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.mapper
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2018/11/15 下午2:06
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class BanSellConfigProvider {

    private static final String TABLE_NAME = " tb_ban_sell_config ";

    private static final String COLUMNS = "id, org_id, account_id, user_name, created , updated";

    public String insert(BanSellConfig banSellConfig) {
        return new SQL() {
            {
                INSERT_INTO(TABLE_NAME);
                if (banSellConfig.getOrgId() != null) {
                    VALUES("org_id", "#{orgId}");
                }
                if (banSellConfig.getAccountId() != null) {
                    VALUES("account_id", "#{accountId}");
                }
                if (banSellConfig.getUserName() != null) {
                    VALUES("user_name", "#{userName}");
                }
                VALUES("created", "#{created}");
                VALUES("updated", "#{updated}");
            }
        }.toString();
    }

    public String deleteByOrgId() {
        return new SQL() {
            {
                DELETE_FROM(TABLE_NAME).WHERE("org_id = #{orgId}");
            }
        }.toString();
    }
}
