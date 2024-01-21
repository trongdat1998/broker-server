package io.bhex.broker.server.primary.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 05/09/2018 5:17 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class SymbolSqlProvider {

    public String count(Map<String, Object> parameter) {
        final List<String> symbolIdList = (List<String>) parameter.get("symbolIdList");
        String quoteToken = (String) parameter.get("quoteToken");
        Long exchangeId = (Long) parameter.get("exchangeId");
        Integer category = (Integer) parameter.get("category");
        String symbolName = (String) parameter.get("symbolName");

        StringBuilder sqlBuilder = new StringBuilder("select count(*) from tb_symbol where org_id = #{brokerId} ");
        if (quoteToken != null && StringUtils.isNotEmpty(quoteToken)) {
            sqlBuilder.append(" and quote_token_id = #{quoteToken} ");
        }
        if (exchangeId != null && exchangeId > 0) {
            sqlBuilder.append(" and exchange_id = #{exchangeId} ");
        }
        if (category != null && category != 0) {
            sqlBuilder.append(" and category = #{category} ");
        }
        if (!CollectionUtils.isEmpty(symbolIdList)) {
            sqlBuilder.append(" and symbol_id in(");
            for (int i = 0; i < symbolIdList.size(); i++) {
                sqlBuilder.append(String.format("#{symbolIdList[%d]}", i));
                if (i < symbolIdList.size() - 1) {
                    sqlBuilder.append(",");
                }
            }
            sqlBuilder.append(")");
        }
        if (StringUtils.isNotEmpty(symbolName)) {
            sqlBuilder.append(" and (" +
                    " symbol_name like CONCAT('%',#{symbolName},'%')" +
                    " OR symbol_id like CONCAT('%',#{symbolName},'%') " +
                    ") ");
        }
        return sqlBuilder.toString();
//        return new SQL() {
//            {
//                SELECT("count(*)").FROM(SymbolMapper.TABLE_NAME);
//                WHERE("org_id = #{brokerId}");
//                if (!CollectionUtils.isEmpty(symbolIdList)) {
//                    WHERE("symbol_id = #{symbolId}");
//
//                }
//                if (quoteToken != null && StringUtils.isNotEmpty(quoteToken)) {
//                    WHERE("quote_token_id = #{quoteToken}");
//                }
//                if (exchangeId != null && exchangeId > 0) {
//                    WHERE("exchange_id = #{exchangeId}");
//                }
//                if (category != null && category != 0) {
//                    WHERE("category = #{category}");
//                }
//            }
//        }.toString();
    }

    public String querySymbol(Map<String, Object> parameter) {
        final List<String> symbolIdList = (List<String>) parameter.get("symbolIdList");
        Long exchangeId = (Long) parameter.get("exchangeId");
        String quoteToken = (String) parameter.get("quoteToken");
        Integer category = (Integer) parameter.get("category");
        String symbolName = (String) parameter.get("symbolName");

        StringBuilder sqlBuilder = new StringBuilder("select * from tb_symbol where org_id = #{brokerId} ");
        if (quoteToken != null && StringUtils.isNotEmpty(quoteToken)) {
            sqlBuilder.append(" and quote_token_id = #{quoteToken} ");
        }
        if (exchangeId != null && exchangeId > 0) {
            sqlBuilder.append(" and exchange_id = #{exchangeId} ");
        }
        if (category != null && category != 0) {
            sqlBuilder.append(" and category = #{category} ");
        }
        if (!CollectionUtils.isEmpty(symbolIdList)) {
            sqlBuilder.append(" and symbol_id in(");
            for (int i = 0; i < symbolIdList.size(); i++) {
                sqlBuilder.append(String.format("#{symbolIdList[%d]}", i));
                if (i < symbolIdList.size() - 1) {
                    sqlBuilder.append(",");
                }
            }
            sqlBuilder.append(")");
        }
        if (StringUtils.isNotEmpty(symbolName)) {
            sqlBuilder.append(" and (" +
                    " symbol_name like CONCAT('%',#{symbolName},'%')" +
                    " OR symbol_id like CONCAT('%',#{symbolName},'%') " +
                    ") ");
        }
        sqlBuilder.append(" order by status desc,show_status desc,custom_order desc,symbol_id asc limit #{fromindex}, #{endindex} ");
        return sqlBuilder.toString();

//        return new SQL() {
//            {
//                SELECT(SymbolMapper.ALL_COLUMNS).FROM(SymbolMapper.TABLE_NAME);
//                WHERE("org_id = #{brokerId}");
//                if (symbolId != null && StringUtils.isNotEmpty(symbolId)) {
//                    WHERE("symbol_id = #{symbolId}");
//                }
//                if (quoteToken != null && StringUtils.isNotEmpty(quoteToken)) {
//                    WHERE("quote_token_id = #{quoteToken}");
//                }
//                if (exchangeId != null && exchangeId > 0) {
//                    WHERE("exchange_id = #{exchangeId}");
//                }
//                if (category != null && category != 0) {
//                    WHERE("category = #{category}");
//                }
//                ORDER_BY(" status desc,show_status desc,custom_order desc,symbol_id asc limit #{fromindex}, #{endindex}");
//            }
//        }.toString();
    }

    public String queryOrgSymbols(Map<String, Object> parameter) {
        Long orgId = (Long) parameter.get("orgId");
        final List<Long> categories = (List<Long>) parameter.get("categories");
        StringBuilder sqlBuilder = new StringBuilder("select " + SymbolMapper.COLUMNS + " from tb_symbol where status = 1 ");
        if (orgId != null && orgId > 0) {
            sqlBuilder.append("and org_id=#{orgId} ");
        }
        if (!CollectionUtils.isEmpty(categories)) {
            sqlBuilder.append("and category in(");
            for (int i = 0; i < categories.size(); i++) {
                sqlBuilder.append(String.format("#{categories[%d]}", i));
                if (i < categories.size() - 1) {
                    sqlBuilder.append(",");
                }
            }
            sqlBuilder.append(")");
        }
        return sqlBuilder.toString();
    }

    public String queryAll(Map<String, Object> parameter) {
        final List<Long> categories = (List<Long>) parameter.get("categories");
        StringBuilder sqlBuilder = new StringBuilder("select " + SymbolMapper.COLUMNS + " from tb_symbol where status = 1 ");
        if (!CollectionUtils.isEmpty(categories)) {
            sqlBuilder.append("and category in(");
            for (int i = 0; i < categories.size(); i++) {
                sqlBuilder.append(String.format("#{categories[%d]}", i));
                if (i < categories.size() - 1) {
                    sqlBuilder.append(",");
                }
            }
            sqlBuilder.append(")");
        }
        return sqlBuilder.toString();
    }
}
