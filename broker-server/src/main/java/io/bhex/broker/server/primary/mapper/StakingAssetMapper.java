package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

import io.bhex.broker.server.domain.staking.StakingAssetOrderAmount;
import io.bhex.broker.server.model.staking.StakingAsset;

@Mapper
public interface StakingAssetMapper extends tk.mybatis.mapper.common.Mapper<StakingAsset> {

    String TABLE_NAME = "tb_staking_asset";

    /**
     * 增加用户理财资产
     *
     * @param orgId
     * @param userId
     * @param productId
     * @param amount
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set total_amount = total_amount + #{amount}, current_amount = current_amount + #{amount}, updated_at = #{updateTime} "
            + " where org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} ")
    int updateStakingAsset(@Param("orgId") Long orgId, @Param("userId") Long userId
            , @Param("productId") Long productId, @Param("amount") BigDecimal amount
            , @Param("updateTime") Long updateTime);

    /**
     * 更新用户收益
     *
     * @param orgId
     * @param userId
     * @param productId
     * @param amount
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set total_profit = total_profit + #{amount},last_profit = #{amount}, updated_at = #{updateTime} "
            + " where org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} ")
    int updateStakingProfit(@Param("orgId") Long orgId, @Param("userId") Long userId
            , @Param("productId") Long productId, @Param("amount") BigDecimal amount
            , @Param("updateTime") Long updateTime);

    /**
     * 扣除用户理财资产
     *
     * @param orgId
     * @param userId
     * @param productId
     * @param amount
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set current_amount = current_amount - #{amount}, updated_at = #{updateTime} "
            + " where org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} and (current_amount - #{amount}) >= 0")
    int reduceStakingAsset(@Param("orgId") Long orgId, @Param("userId") Long userId
            , @Param("productId") Long productId, @Param("amount") BigDecimal amount
            , @Param("updateTime") Long updateTime);

    /**
     * 归还用户理财资产
     *
     * @param orgId
     * @param userId
     * @param productId
     * @param amount
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set current_amount = current_amount + #{amount}, updated_at = #{updateTime} "
            + " where org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} ")
    int returnStakingAsset(@Param("orgId") Long orgId, @Param("userId") Long userId
            , @Param("productId") Long productId, @Param("amount") BigDecimal amount
            , @Param("updateTime") Long updateTime);

    /**
     * 获取活期用户资产
     *
     * @param orgId
     * @param productId
     * @param productType
     * @param limit
     * @return
     */
    @Select(" SELECT * FROM " + TABLE_NAME + " a WHERE org_id = #{orgId} and product_id = #{productId} and product_type = #{productType} "
            + "and current_amount > 0 "
            + " and not exists (select b.id from tb_staking_product_rebate_detail b "
            + " where a.org_id=b.org_id and a.product_id=b.product_id and a.user_id=b.user_id and b.order_id = a.id)"
            + " order by id limit #{limit}")
    List<StakingAsset> listCurrentAsset(@Param("orgId") Long orgId, @Param("productId") Long productId
            , @Param("productType") Integer productType, @Param("limit") Integer limit);


    @Select("select * from tb_staking_asset where org_id = #{orgId} and product_id = #{productId} and (#{userId} = 0 or user_id = #{userId}) and (#{startId} = 0 or id < #{startId}) order by id desc limit #{limit};")
    List<StakingAsset> queryStakingProductAsset(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("userId") Long userId, @Param("startId") Long startId, @Param("limit") Integer limit);

    /**
     * 获取资产大于0的记录
     *
     * @param orgId
     * @param productId
     * @param startId
     * @param limit
     * @return
     */
    @Select("select * from tb_staking_asset t1 where org_id = #{orgId} and product_id = #{productId} and id>#{startId} and current_amount>0 "
            + " and not exists(select t2.id from tb_staking_product_order t2 where t1.org_id=t2.org_id and t1.product_id=t2.product_id and t1.user_id=t2.user_id and t2.order_type=2) "
            + " order by id limit #{limit};")
    List<StakingAsset> listAsset(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("startId") Long startId, @Param("limit") Integer limit);


    /**
     * 获取资产和订单合计金额，用于资产订单数据校验
     *
     * @param orgId
     * @param productId
     * @return
     */
    @Select("select t2.org_id orgId,t2.product_id productId,t2.user_id userId,t2.current_amount assetAmount,t1.orderAmount " +
            "from ( " +
            " select org_id,product_id,user_id,sum(pay_amount) orderAmount from tb_staking_product_order where org_id=#{orgId} and product_id=#{productId} and product_type=1 and status=1 group by org_id,product_id,user_id " +
            ") t1, tb_staking_asset t2 " +
            "where t1.org_id=t2.org_id and t1.product_id=t2.product_id and t1.user_id=t2.user_id and t1.orderAmount != t2.current_amount and t2.product_id=#{productId};")
    List<StakingAssetOrderAmount> queryStakingAssetOrderAmount(@Param("orgId") Long orgId, @Param("productId") Long productId);

    @Select("select ifnull(sum(current_amount),0) from tb_staking_asset where org_id = #{orgId} and user_id = #{userId} and token_id = #{tokenId}")
    BigDecimal queryCurrentAmount(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("tokenId") String tokenId);

    @Select("select * from tb_staking_asset where org_id = #{orgId} and token_id = #{tokenId} and current_amount > 0")
    List<StakingAsset> queryCurrentAmountList(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);
}
