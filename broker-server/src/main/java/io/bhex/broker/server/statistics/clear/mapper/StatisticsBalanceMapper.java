package io.bhex.broker.server.statistics.clear.mapper;

import io.bhex.broker.server.model.OrgBalanceSummary;
import io.bhex.broker.server.model.StatisticsBalance;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface StatisticsBalanceMapper {

    @Select({"<script>"
            , "SELECT a.balance_id, a.org_id, a.broker_user_id as user_id, a.account_id, a.token_id, a.total, a.available, a.locked, ifnull(c.total, 0) as position, a.created_at, a.updated_at"
            , "FROM ods_balance a "
            , "LEFT JOIN ods_position c ON a.account_id collate utf8mb4_0900_ai_ci = c.account_id collate utf8mb4_0900_ai_ci AND a.token_id collate utf8mb4_0900_ai_ci = c.token_id collate utf8mb4_0900_ai_ci "
            , "WHERE a.org_id=#{orgId} AND a.token_id=#{tokenId} "
            , "<if test=\"userId != null and userId &gt; 0\">AND a.broker_user_id = #{userId}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.balance_id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.balance_id &gt; #{lastId}</if> "
            , "AND a.total > 0 "
            , "ORDER BY a.balance_id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit} "
            , "</script>"})
    List<StatisticsBalance> queryOrgTokenBalance(@Param("orgId") Long orgId,
                                                 @Param("userId") Long userId,
                                                 @Param("tokenId") String tokenId,
                                                 @Param("fromId") Long fromBalanceId,
                                                 @Param("lastId") Long endBalanceId,
                                                 @Param("limit") Integer limit,
                                                 @Param("orderDesc") Boolean orderDesc);

    @Select("SELECT token_id, sum(total) as total_sum, sum(available) as available_sum, sum(locked) as locked_sum FROM ods_balance WHERE org_id=#{orgId} GROUP BY token_id having sum(total) > 0 ORDER BY token_id")
    List<OrgBalanceSummary> orgBalanceSummary(@Param("orgId") Long orgId);

    @Select({"<script>"
            , "SELECT a.balance_id, a.org_id, b.broker_user_id as user_id, a.account_id, a.token_id, a.total, a.available, a.locked, ifnull(c.total, 0) as position, a.created_at, a.updated_at "
            , "FROM ods_balance a "
            , "JOIN ods_account b ON a.account_id = b.account_id "
            , "LEFT JOIN ods_position c ON a.account_id collate utf8mb4_0900_ai_ci = c.account_id collate utf8mb4_0900_ai_ci AND a.token_id collate utf8mb4_0900_ai_ci = c.token_id collate utf8mb4_0900_ai_ci "
            , "WHERE a.org_id=#{orgId} AND a.token_id=#{tokenId} "
            , "ORDER BY a.total DESC LIMIT #{top} "
            , "</script>"})
    List<StatisticsBalance> queryOrgTokenBalanceTop(@Param("orgId") Long orgId,
                                                    @Param("tokenId") String tokenId,
                                                    @Param("top") Integer top);


    @Select({"<script>"
            , " SELECT  balance_id,org_id,broker_user_id as user_id,token_id,total FROM ods_balance "
            , " WHERE org_id = #{orgId} AND total > 0 "
            , "<if test=\"tokenId != null and tokenId != '' \"> AND token_id = #{tokenId} </if> "
            , "<if test=\"userId != null and userId &gt; 0\"> AND broker_user_id = #{userId} </if> "
            , " ORDER BY total DESC LIMIT #{top} "
            , "</script>"})
    List<StatisticsBalance> queryAdminOrgTokenBalanceTop(@Param("orgId") Long orgId, @Param("tokenId") String tokenId,
                                                         @Param("userId") Long userId, @Param("top") Integer top);


}
