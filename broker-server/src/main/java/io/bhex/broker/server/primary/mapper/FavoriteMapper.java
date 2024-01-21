package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.Favorite;
import org.apache.ibatis.annotations.*;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

@Mapper
public interface FavoriteMapper extends InsertListMapper<Favorite> {

    String COLUMNS = "id, org_id, user_id, exchange_id, symbol_id, custom_order, created";

    @Insert("INSERT INTO tb_favorite(org_id, user_id, exchange_id, symbol_id, created, custom_order) VALUES(#{orgId}, #{userId}, #{exchangeId}, #{symbolId}, #{created}, #{customOrder})")
    @Options(useGeneratedKeys = true, keyColumn = "id")
    int insert(Favorite favorite);

    @Delete("DELETE FROM tb_favorite WHERE user_id = #{userId} AND exchange_id = #{exchangeId} AND symbol_id = #{symbolId}")
    int delete(@Param("userId") Long userId, @Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId);

    @Delete("DELETE FROM tb_favorite WHERE org_id = #{orgId} AND user_id = #{userId}")
    int deleteMyFavorites(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select("SELECT " + COLUMNS + " FROM tb_favorite WHERE org_id = #{orgId} AND user_id = #{userId} order by custom_order desc")
    List<Favorite> queryByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select("SELECT COUNT(1) FROM tb_favorite WHERE user_id = #{userId} AND exchange_id = #{exchangeId} AND symbol_id = #{symbolId}")
    int countIfExists(@Param("userId") Long userId, @Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId);

    @Select("SELECT custom_order FROM tb_favorite WHERE org_id = #{orgId} AND user_id = #{userId} order by custom_order desc limit 1")
    Integer getMaxCustomerOrder(@Param("orgId") Long orgId, @Param("userId") Long userId);

    /**
     * userId和symbolId是唯一索引，所以不用去重
     */
    @Select("SELECT user_id FROM tb_favorite WHERE org_id = #{orgId} AND symbol_id = #{symbolId} AND id > #{id} order by id asc LIMIT #{limit}")
    List<Long> queryUserIdsBySymbolId(@Param("orgId") Long orgId, @Param("symbolId") String symbolId, @Param("id") Long id, @Param("limit") Integer limit);

    @Select("SELECT DISTINCT symbol_id FROM tb_favorite WHERE org_id = #{orgId}")
    List<String> querySymbolIdsByOrgId(@Param("orgId") Long orgId);
}

