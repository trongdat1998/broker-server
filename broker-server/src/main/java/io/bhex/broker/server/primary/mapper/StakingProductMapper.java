package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingProduct;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface StakingProductMapper extends tk.mybatis.mapper.common.Mapper<StakingProduct> {

    String TABLE_NAME = "tb_staking_product";

    /**
     * 更新理财产品已售数量
     * @param id
     * @param soldLots
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set sold_lots = sold_lots + #{soldLots}, updated_at = #{updateTime} where id = #{id} and org_id=#{orgId} and (sold_lots+#{soldLots})<=up_limit_lots")
    int updateStakingProductSoldLots(@Param("id") Long id, @Param("orgId") Long orgId, @Param("soldLots") Integer soldLots, @Param("updateTime") Long updateTime);

    /**
     * 更新理财产品已售数量
     * @param id
     * @param soldLots
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set sold_lots = sold_lots - #{soldLots}, updated_at = #{updateTime} where id = #{id} and org_id=#{orgId}")
    int releaseStakingProductSoldLots(@Param("id") Long id, @Param("orgId") Long orgId, @Param("soldLots") Integer soldLots, @Param("updateTime") Long updateTime);

    @Select("SELECT * FROM " + TABLE_NAME + " WHERE is_show = #{is_show} AND (up_limit_lots-sold_lots) > 0")
    List<StakingProduct> listSubscribeProduct(@Param("show") Integer show);

    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} AND id = #{productId}")
    StakingProduct getProductById(@Param("orgId") Long orgId,@Param("productId") Long productId);

}
