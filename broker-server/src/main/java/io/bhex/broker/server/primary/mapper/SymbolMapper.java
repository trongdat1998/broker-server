package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.Symbol;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 20/08/2018 9:18 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
@Component
public interface SymbolMapper extends Mapper<Symbol> {

    String TABLE_NAME = " tb_symbol ";

    String ALL_COLUMNS = "id, org_id, exchange_id, symbol_id, symbol_name, base_token_id, base_token_name, quote_token_id, quote_token_name, "
            + "allow_trade, status, custom_order, index_show, index_show_order, ban_sell_status,ban_buy_status,show_status, need_preview_check, "
            + "is_aggregate, open_time, category,index_recommend_order,allow_margin, filter_time, "
            + "filter_top_status, label_id, hide_from_openapi, forbid_openapi_trade, allow_plan,extra_tag,extra_config ";

    String COLUMNS = "id, org_id, exchange_id, symbol_id, symbol_name, base_token_id, base_token_name, quote_token_id, quote_token_name, "
            + "allow_trade, status, custom_order, index_show, index_show_order, ban_sell_status,ban_buy_status,show_status, need_preview_check, "
            + "is_aggregate, open_time, category,index_recommend_order,allow_margin,filter_time, "
            + "filter_top_status, label_id, hide_from_openapi, forbid_openapi_trade, allow_plan,extra_tag,extra_config ";



    @SelectProvider(type = SymbolSqlProvider.class, method = "count")
    int countByBrokerId(@Param("brokerId") Long brokerId, @Param("exchangeId") Long exchangeId, @Param("quoteToken") String quoteToken,
                        @Param("symbolIdList") List<String> symbolIdList, @Param("symbolName") String symbolName, @Param("category") Integer category);

    @SelectProvider(type = SymbolSqlProvider.class, method = "querySymbol")
    List<Symbol> querySymbol(@Param("fromindex") Integer fromindex, @Param("endindex") Integer endindex, @Param("brokerId") Long brokerId,
                             @Param("exchangeId") Long exchangeId, @Param("quoteToken") String quoteToken,
                             @Param("symbolIdList") List<String> symbolIdList, @Param("symbolName") String symbolName, @Param("category") Integer category);

    @Select("SELECT " + ALL_COLUMNS + " FROM tb_symbol where exchange_id=#{exchangeId} and symbol_id=#{symbolId} and org_id=#{orgId}")
    Symbol getBySymbolId(@Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId, @Param("orgId") Long orgId);

    @Select("SELECT " + ALL_COLUMNS + " FROM tb_symbol where org_id=#{orgId} and symbol_name=#{symbolName}")
    Symbol getBySymbolName(@Param("orgId") Long orgId, @Param("symbolName") String symbolName);

    @Select("SELECT " + ALL_COLUMNS + " FROM tb_symbol where org_id=#{orgId} and symbol_id=#{symbolId}")
    Symbol getOrgSymbol(@Param("orgId") Long orgId, @Param("symbolId") String symbolId);

    @Update("update " + TABLE_NAME + " set allow_trade=#{allowTrade}, updated=unix_timestamp() * 1000 where exchange_id=#{exchangeId} and symbol_id=#{symbolId} and org_id=#{brokerId}")
    int allowTrade(@Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId, @Param("allowTrade") Integer allowTrade, @Param("brokerId") Long brokerId);

    @Update("update " + TABLE_NAME + " set status=#{isPublished}, updated=unix_timestamp() * 1000 where exchange_id=#{exchangeId} and symbol_id=#{symbolId} and org_id=#{brokerId}")
    int publish(@Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId, @Param("isPublished") Integer isPublished, @Param("brokerId") Long brokerId);

    @SelectProvider(type = SymbolSqlProvider.class, method = "queryOrgSymbols")
    List<Symbol> queryOrgSymbols(@Param("orgId") Long orgId, @Param("categories") List<Integer> categories);

    @SelectProvider(type = SymbolSqlProvider.class, method = "queryAll")
    List<Symbol> queryAll(@Param("categories") List<Integer> categories);

    @Update("update " + TABLE_NAME + " set ban_sell_status=#{banSellStatus} where exchange_id=#{exchangeId} and symbol_id=#{symbolId} and org_id=#{brokerId}")
    int updateBanType(@Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId, @Param("banSellStatus") Integer banSellStatus, @Param("brokerId") Long brokerId);

    @Select(" <script> "
            + " SELECT " + ALL_COLUMNS + " FROM " + TABLE_NAME + " WHERE exchange_id=#{exchangeId} and org_id=#{brokerId} and symbol_id in "
            + " <foreach item='item' index='index' collection='symbolIds' open='(' separator=',' close=')'> "
            + " #{item} "
            + " </foreach> "
            + " </script> ")
    List<Symbol> queryBySymbolIds(@Param("exchangeId") Long exchangeId, @Param("brokerId") Long brokerId, @Param("symbolIds") List<String> symbolIds);

    @Select("SELECT id FROM tb_symbol where symbol_id=#{symbolId} and org_id=#{orgId}")
    Long getIdBySymbolId(@Param("symbolId") String symbolId, @Param("orgId") Long orgId);

    @Select("SELECT exchange_id FROM tb_symbol where org_id=#{orgId} and symbol_id=#{symbolId}")
    Long getExchangeIdBySymbolId(@Param("orgId") Long orgId, @Param("symbolId") String symbolId);

    @Update("update " + TABLE_NAME + " set index_recommend_order=0 where org_id=#{orgId}")
    int resetRecommends(@Param("orgId") Long orgId);

    @Update("update " + TABLE_NAME + " set index_recommend_order=#{position} where symbol_id=#{symbolId} and org_id=#{orgId}")
    int updateRecommendOrder(@Param("orgId") Long orgId, @Param("symbolId") String symbolId, @Param("position") int position);

    @Select("select symbol_id from " + TABLE_NAME + " where org_id=#{orgId} and index_recommend_order > 0 order by index_recommend_order desc")
    List<String> getRecommendSymbols(@Param("orgId") Long orgId);

    //下线交易所的币对
    @Update("update " + TABLE_NAME + " set status=0, updated=unix_timestamp() * 1000 where exchange_id=#{exchangeId} and symbol_id=#{symbolId}")
    int disableByExchangeId(@Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId);


    @Update({"<script>"
            , "update " + TABLE_NAME + " set custom_order = 0 where org_id = #{orgId}  and category = #{category} "
            , "<if test=\"quoteTokenId != null and quoteTokenId != '' \"> AND quote_token_id = #{quoteTokenId} </if> "
            , "</script>"})
    int resetQuoteSymbolCustomerOrder(@Param("orgId") Long orgId, @Param("quoteTokenId") String quoteTokenId, @Param("category") Integer category);

    @Update({"<script>"
            , "update " + TABLE_NAME + " set custom_order=#{position} where symbol_id=#{symbolId} and org_id=#{orgId} and category = #{category} "
            , "<if test=\"quoteTokenId != null and quoteTokenId != '' \"> AND quote_token_id = #{quoteTokenId} </if> "
            , "</script>"})
    int updateQuoteSymbolCustomerOrder(@Param("orgId") Long orgId, @Param("quoteTokenId") String quoteTokenId, @Param("symbolId") String symbolId, @Param("position") int position, @Param("category") Integer category);

    @Select({"<script>"
            , "select base_token_id, quote_token_id, symbol_id from " + TABLE_NAME + " where org_id=#{orgId} "
            , "<if test=\"quoteTokenId != null and quoteTokenId != '' \"> AND quote_token_id = #{quoteTokenId} </if> "
            , "and category = #{category} and status = 1 order by show_status desc,custom_order desc,symbol_id asc "
            , "</script>"})
    List<Symbol> getQuoteSymbols(@Param("orgId") Long orgId, @Param("quoteTokenId") String quoteTokenId, @Param("category") Integer category);

    @Update("update " + TABLE_NAME + " set allow_margin=#{allowMargin}, updated=unix_timestamp() * 1000 where exchange_id=#{exchangeId} and symbol_id=#{symbolId} and org_id=#{orgId}")
    int allowMargin(@Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId, @Param("allowMargin") Integer allowMargin, @Param("orgId") Long orgId);

    @Update("update " + TABLE_NAME + " set status=0, updated=unix_timestamp() * 1000 where org_id=#{orgId} and status != 0")
    int removeBrokerSymbols(@Param("orgId") Long orgId);

    @Update("update " + TABLE_NAME + " set filter_time=#{filterTime} where org_id=#{orgId} and symbol_id=#{symbolId}")
    int updateSymbolFilterTime(@Param("orgId") Long orgId, @Param("symbolId") String symbolId, @Param("filterTime") Long filterTime);

    @Update("update " + TABLE_NAME + " set label_id=#{labelId}, updated=unix_timestamp() * 1000 where org_id=#{orgId} and symbol_id=#{symbolId}")
    int setSymbolLabel(@Param("orgId") Long orgId, @Param("symbolId") String symbolId, @Param("labelId") Long labelId);

    @Update("update " + TABLE_NAME + " set hide_from_openapi=#{hideFromOpenapi}, updated=unix_timestamp() * 1000 where org_id=#{orgId} and symbol_id=#{symbolId}")
    int hideSymbolFromOpenapi(@Param("orgId") Long orgId, @Param("symbolId") String symbolId, @Param("hideFromOpenapi") Integer hideFromOpenapi);

    @Update("update " + TABLE_NAME + " set forbid_openapi_trade=#{forbidOpenapiTrade}, updated=unix_timestamp() * 1000 where org_id=#{orgId} and symbol_id=#{symbolId}")
    int forbidOpenapiTrade(@Param("orgId") Long orgId, @Param("symbolId") String symbolId, @Param("forbidOpenapiTrade") Integer forbidOpenapiTrade);

    @Update("update " + TABLE_NAME + " set base_token_name=#{tokenName},symbol_name=concat(#{tokenName},quote_token_name), updated=unix_timestamp() * 1000 where org_id=#{orgId} and base_token_id=#{tokenId}")
    int updateBaseTokenName(@Param("orgId") Long orgId, @Param("tokenId") String tokenId, @Param("tokenName") String newTokenName);

    @Update("update " + TABLE_NAME + " set quote_token_name=#{tokenName},symbol_name=concat(base_token_name,#{tokenName}), updated=unix_timestamp() * 1000 where org_id=#{orgId} and quote_token_id=#{tokenId}")
    int updateQuoteTokenName(@Param("orgId") Long orgId, @Param("tokenId") String tokenId, @Param("tokenName") String newTokenName);
}
