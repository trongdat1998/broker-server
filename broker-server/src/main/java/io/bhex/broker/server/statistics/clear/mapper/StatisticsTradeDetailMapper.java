package io.bhex.broker.server.statistics.clear.mapper;

import io.bhex.broker.server.model.*;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.sql.Timestamp;
import java.util.List;

@Mapper
public interface StatisticsTradeDetailMapper extends tk.mybatis.mapper.common.Mapper<StatisticsTradeAllDetail> {

    @Select({"<script>"
            , "SELECT "
            + "     a.symbol_id, "
            + "     if(a.is_actually_used_sys_token, a.sys_token_id, if(a.side, b.quote_token_id, if(b.security_type in (2, 3), b.quote_token_id, b.base_token_id))) fee_token_id, "
            + "     sum(if(a.is_actually_used_sys_token, a.sys_token_fee, a.token_fee)) fee_total "
            , "FROM ods_trade_detail a "
            + "JOIN ods_symbol b "
            + "ON a.symbol_id = b.symbol_id "
            , "WHERE a.broker_id=#{orgId} "
            , "<if test=\"accountId != null and accountId &gt; 0\">AND a.account_id = #{accountId}</if> "
            , "<if test=\"symbolId != null and symbolId != ''\">AND a.symbol_id = #{symbolId}</if> "
            , "<if test=\"startTime != null\">AND a.created_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND a.created_at &lt;= #{endTime}</if> "
            , "AND a.status=1 "
            , "GROUP BY a.symbol_id, fee_token_id "
            , "</script>"
    })
    List<StatisticsSymbolTradeFee> statisticsSymbolTradeFee(@Param("orgId") Long orgId,
                                                            @Param("accountId") Long accountId,
                                                            @Param("symbolId") String symbolId,
                                                            @Param("startTime") Timestamp startTime,
                                                            @Param("endTime") Timestamp endTime);

    @Select({"<script>"
            , "SELECT "
            + "     a.broker_user_id, a.account_id, "
            + "     sum(if(a.is_actually_used_sys_token, a.sys_token_fee, a.token_fee)) fee_total "
            , "FROM ods_trade_detail a "
            + "JOIN ods_symbol b "
            + "ON a.symbol_id = b.symbol_id "
            , "WHERE a.broker_id=#{orgId} "
            , "AND if(a.is_actually_used_sys_token, a.sys_token_id, if(a.side, b.quote_token_id, if(b.security_type in (2, 3), b.quote_token_id, b.base_token_id))) = #{feeTokenId} "
            , "<if test=\"symbolId != null and symbolId != ''\">AND a.symbol_id = #{symbolId}</if> "
            , "<if test=\"startTime != null\">AND a.created_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND a.created_at &lt;= #{endTime}</if> "
            , "AND a.status=1 "
            , "GROUP BY a.broker_user_id "
            , "ORDER BY fee_total DESC limit #{top} "
            , "</script>"
    })
    List<StatisticsTradeFeeTop> statisticsTradeFeeTop(@Param("orgId") Long orgId,
                                                      @Param("feeTokenId") String feeTokenId,
                                                      @Param("symbolId") String symbolId,
                                                      @Param("startTime") Timestamp startTime,
                                                      @Param("endTime") Timestamp endTime,
                                                      @Param("top") Integer top);

    @Select({"<script>"
            , "SELECT "
            + "     a.trade_detail_id as trade_id, a.broker_id as org_id, a.order_id, a.broker_user_id as user_id, a.account_id, a.symbol_id, a.order_type, a.side, a.price, a.quantity, a.amount, "
            + "     if(a.is_actually_used_sys_token, a.sys_token_id, if(a.side, b.quote_token_id, if(b.security_type in (2, 3), b.quote_token_id, b.base_token_id))) as fee_token, "
            + "     if(a.is_actually_used_sys_token, a.sys_token_fee, a.token_fee) as fee, a.created_at as match_time, a.updated_at as created_at, a.updated_at "
            , "FROM ods_trade_detail a "
            + "JOIN ods_symbol b "
            + "ON a.symbol_id = b.symbol_id "
            , "WHERE a.broker_id=#{orgId} "
            , "<if test=\"symbolId != null and symbolId != ''\">AND a.symbol_id = #{symbolId}</if> "
            , "<if test=\"startTime != null\">AND a.updated_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND a.updated_at &lt;= #{endTime}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.trade_detail_id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.trade_detail_id &gt; #{lastId}</if> "
            , "AND a.status=1 "
            , "ORDER BY a.trade_detail_id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit} "
            , "</script>"
    })
    List<StatisticsTradeDetail> queryOrgTradeDetail(@Param("orgId") Long orgId,
                                                    @Param("symbolId") String symbolId,
                                                    @Param("startTime") Timestamp startTime,
                                                    @Param("endTime") Timestamp endTime,
                                                    @Param("fromId") Long fromId,
                                                    @Param("lastId") Long lastId,
                                                    @Param("limit") Integer limit,
                                                    @Param("orderDesc") Boolean orderDesc);

    @Select({"<script>"
            , "SELECT "
            + "     a.ticket_id, a.trade_detail_id as trade_id, a.broker_id as org_id, a.order_id, a.broker_user_id as user_id, a.account_id, a.symbol_id, a.order_type, a.side, a.price, a.quantity, a.amount, "
            + "     a.is_actually_used_sys_token, a.sys_token_id, "
            + "     if(a.is_actually_used_sys_token, a.sys_token_fee, a.token_fee) as fee, a.created_at as match_time, a.updated_at as created_at, a.updated_at "
            , "FROM ods_trade_detail a "
            , "WHERE a.broker_id=#{orgId} "
            , "<if test=\"symbolId != null and symbolId != ''\">AND a.symbol_id = #{symbolId}</if> "
            , "<if test=\"startTime != null\">AND a.updated_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND a.updated_at &lt;= #{endTime}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.trade_detail_id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.trade_detail_id &gt; #{lastId}</if> "
            , "AND a.status=1 "
            , "ORDER BY a.trade_detail_id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit} "
            , "</script>"
    })
    List<StatisticsTradeDetail> queryOnlyOrgTradeDetail(@Param("orgId") Long orgId,
                                                        @Param("symbolId") String symbolId,
                                                        @Param("startTime") Timestamp startTime,
                                                        @Param("endTime") Timestamp endTime,
                                                        @Param("fromId") Long fromId,
                                                        @Param("lastId") Long lastId,
                                                        @Param("limit") Integer limit,
                                                        @Param("orderDesc") Boolean orderDesc);


    @Select({"<script>"
            , "SELECT "
            + "     a.ticket_id, a.trade_detail_id as trade_id, a.broker_id as org_id, a.order_id, a.broker_user_id as user_id, a.account_id "
            , "FROM ods_trade_detail a "
            , "WHERE a.ticket_id in "
            , " <foreach item='item' index='index' collection='ticketIds' open='(' separator=',' close=')'> "
            , "     #{item} "
            , " </foreach> "
            , "</script>"
    })
    List<StatisticsTradeDetail> queryOrgTradeDetailByTicketId(@Param("ticketIds") List<Long> ticketIds);

//    @Select({"<script>"
//            , "SELECT "
//            + "     a.trade_detail_id as trade_id, a.broker_id as org_id, a.order_id, a.broker_user_id as user_id, a.account_id, a.symbol_id, a.order_type, a.side, a.price, a.quantity, a.amount, "
//            + "     if(a.is_actually_used_sys_token, a.sys_token_id, '') as fee_token, "
//            + "     if(a.is_actually_used_sys_token, a.sys_token_fee, a.token_fee) as fee, a.created_at as match_time, a.created_at, a.updated_at "
//            , "FROM ods_trade_detail a "
//            , "WHERE a.broker_id=#{orgId} "
//            , "<if test=\"symbolId != null and symbolId != ''\">AND a.symbol_id = #{symbolId}</if> "
//            , "<if test=\"startTime != null\">AND a.created_at &gt;= #{startTime}</if> "
//            , "<if test=\"endTime != null\">AND a.created_at &lt;= #{endTime}</if> "
//            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.trade_detail_id &lt; #{fromId}</if> "
//            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.trade_detail_id &gt; #{lastId}</if> "
//            , "AND a.status=1 "
//            , "ORDER BY a.created_at <if test=\"orderDesc\">DESC</if> "
//            , "LIMIT #{limit} "
//            , "</script>"
//    })
//    List<StatisticsTradeDetail> queryOrgTradeDetail(@Param("orgId") Long orgId,
//                                                    @Param("symbolId") String symbolId,
//                                                    @Param("startTime") Timestamp startTime,
//                                                    @Param("endTime") Timestamp endTime,
//                                                    @Param("fromId") Long fromId,
//                                                    @Param("lastId") Long lastId,
//                                                    @Param("limit") Integer limit,
//                                                    @Param("orderDesc") Boolean orderDesc);

    @Select({"<script>"
            , "SELECT symbol_id, sum(quantity) as total_quantity, sum(amount) as total_amount, round(avg(price), 8) as avg_price "
            , "FROM ods_trade_detail a "
            , "WHERE a.broker_id=#{orgId} "
            , "<if test=\"symbolId != null and symbolId != ''\">AND symbol_id = #{symbolId}</if> "
            , "<if test=\"startTime != null\">AND created_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND created_at &lt;= #{endTime}</if> "
            , "AND status=1 "
            , "GROUP BY a.symbol_id "
            , "</script>"
    })
    List<SymbolTradeInfo> querySymbolTradeInfo(@Param("orgId") Long orgId,
                                               @Param("symbolId") String symbolId,
                                               @Param("startTime") Timestamp startTime,
                                               @Param("endTime") Timestamp endTime);


    @Select({"<script>"
            , "SELECT symbol_id, side, sum(quantity) as total_quantity, sum(amount) as total_amount, round(avg(price), 2) as avg_price "
            , "FROM ods_trade_detail a "
            , "WHERE a.broker_id=#{orgId} "
            , "<if test=\"symbolId != null and symbolId != ''\">AND symbol_id = #{symbolId}</if> "
            , "<if test=\"startTime != null\">AND created_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND created_at &lt;= #{endTime}</if> "
            , "AND status=1 "
            , "GROUP BY a.symbol_id, side"
            , "</script>"
    })
    List<SymbolTradeInfo> querySymbolTradeInfoWithOrderSide(@Param("orgId") Long orgId,
                                                            @Param("symbolId") String symbolId,
                                                            @Param("startTime") Timestamp startTime,
                                                            @Param("endTime") Timestamp endTime,
                                                            @Param("orderSide") Integer orderSide);

    @Select("select * from ods_trade_detail where broker_id = #{orgId} and updated_at > #{time} order by trade_detail_id asc limit #{page},#{size}")
    List<StatisticsTradeAllDetail> queryTradeDetailByOrgId(@Param("orgId") Long orgId,
                                                           @Param("time") String time,
                                                           @Param("page") Integer page,
                                                           @Param("size") Integer size);
}
