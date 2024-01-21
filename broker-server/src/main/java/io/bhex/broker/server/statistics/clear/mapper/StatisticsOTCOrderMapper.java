package io.bhex.broker.server.statistics.clear.mapper;

import io.bhex.broker.server.model.StatisticsOTCOrder;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.sql.Timestamp;
import java.util.List;

@Mapper
public interface StatisticsOTCOrderMapper {

    @Select({"<script>"
            , "SELECT id as order_id, org_id, user_id, account_id, side, token_id, currency_id, price, quantity, amount, fee, payment_type, status, transfer_date, create_date, update_date "
            , "FROM ods_otc_order "
            , "WHERE org_id=#{orgId} "
            , "<if test=\"userId != null and userId &gt; 0\">AND user_id = #{userId}</if> "
            , "<if test=\"tokenId != null and tokenId != ''\">AND token_id = #{tokenId}</if> "
            , "<if test=\"startTime != null\">AND create_date &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND create_date &lt;= #{endTime}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND id &gt; #{lastId}</if> "
            , "ORDER BY id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit} "
            , "</script>"})
    List<StatisticsOTCOrder> queryOrgOTCOrder(@Param("orgId") Long orgId,
                                              @Param("userId") Long userId,
                                              @Param("tokenId") String tokenId,
                                              @Param("startTime") Timestamp startTime,
                                              @Param("endTime") Timestamp endTime,
                                              @Param("fromId") Long fromId,
                                              @Param("lastId") Long lastId,
                                              @Param("limit") Integer limit,
                                              @Param("orderDesc") Boolean orderDesc);

}
