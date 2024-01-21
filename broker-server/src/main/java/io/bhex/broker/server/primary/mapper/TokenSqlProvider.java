package io.bhex.broker.server.primary.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;

import java.util.List;
import java.util.Map;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 04/09/2018 6:10 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
public class TokenSqlProvider {

    public String count(Map<String, Object> parameter) {

        String tokenId = (String) parameter.get("tokenId");
        String tokenName = (String) parameter.get("tokenName");
        Integer category = (Integer) parameter.get("category");
        return new SQL() {
            {
                SELECT("count(*)").FROM(TokenMapper.TABLE_NAME);
                WHERE("org_id = #{brokerId}");
                if (tokenId != null && StringUtils.isNotEmpty(tokenId)) {
                    WHERE("token_id = #{tokenId}");
                } else if (tokenName != null && StringUtils.isNotEmpty(tokenName)) {
                    WHERE("token_name like CONCAT('%',#{tokenName},'%')");
                }
                if (category != null && category != 0) {
                    WHERE("category = #{category}");
                }
            }
        }.toString();
    }

    public String queryToken(Map<String, Object> parameter) {

        String tokenId = (String) parameter.get("tokenId");
        String tokenName = (String) parameter.get("tokenName");
        Integer category = (Integer) parameter.get("category");
        return new SQL() {
            {
                SELECT(TokenMapper.COLUMNS).FROM(TokenMapper.TABLE_NAME);
                WHERE("org_id = #{brokerId}");
                if (tokenId != null && StringUtils.isNotEmpty(tokenId)) {
                    WHERE("token_id = #{tokenId}");
                } else if (tokenName != null && StringUtils.isNotEmpty(tokenName)) {
                    WHERE("(" +
                            " token_name like CONCAT('%',#{tokenName},'%') " +
                            " OR token_full_name like CONCAT('%',#{tokenName},'%') " +
                            " OR token_id like CONCAT('%',#{tokenName},'%') " +
                            ")");
                }
                if (category != null && category != 0) {
                    WHERE("category = #{category}");
                }
                ORDER_BY("id DESC limit #{fromindex}, #{endindex}");
            }
        }.toString();
    }

    public String queryAll(Map<String, Object> parameter) {
        final List<Long> categories = (List<Long>) parameter.get("categories");

        StringBuilder sqlBuilder = new StringBuilder("select " + TokenMapper.COLUMNS + " from tb_token where status = 1 and category in(");
        for (int i = 0; i < categories.size(); i++) {
            sqlBuilder.append(String.format("#{categories[%d]}", i));
            if (i < categories.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    public String queryAllByOrgId(Map<String, Object> parameter) {
        final List<Long> categories = (List<Long>) parameter.get("categories");
        StringBuilder sqlBuilder = new StringBuilder("select " + TokenMapper.COLUMNS + " from tb_token where status = 1 and org_id=#{orgId} and category in(");
        for (int i = 0; i < categories.size(); i++) {
            sqlBuilder.append(String.format("#{categories[%d]}", i));
            if (i < categories.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    public String queryQuoteTokens(Map<String, Object> parameter) {
        final List<Long> categories = (List<Long>) parameter.get("categories");

        StringBuilder sqlBuilder = new StringBuilder("select " + TokenMapper.QUOTE_TOKEN_COLUMNS + " from tb_quote_token where status = 1 and category in(");
        for (int i = 0; i < categories.size(); i++) {
            sqlBuilder.append(String.format("#{categories[%d]}", i));
            if (i < categories.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    public String queryQuoteTokensByOrgId(Map<String, Object> parameter) {
        final List<Long> categories = (List<Long>) parameter.get("categories");

        StringBuilder sqlBuilder = new StringBuilder("select " + TokenMapper.QUOTE_TOKEN_COLUMNS + " from tb_quote_token where status = 1 and org_id=#{orgId} and category in(");
        for (int i = 0; i < categories.size(); i++) {
            sqlBuilder.append(String.format("#{categories[%d]}", i));
            if (i < categories.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

}
