package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingAsset;
import io.bhex.broker.server.model.staking.StakingAssetSnapshot;
import org.apache.ibatis.annotations.*;

import java.math.BigDecimal;
import java.util.List;

/**
 * 资产快照
 *
 * @author generator
 * @date 2020-08-21
 */
@Mapper
public interface StakingAssetSnapshotMapper extends tk.mybatis.mapper.common.Mapper<StakingAssetSnapshot> {

    String TABLE_NAME = "tb_staking_asset_snapshot";
    String FIELDS = " id,org_id,user_id,product_id,token_id,daily_date,net_asset,pay_interest,apr,status ";

    /**
     * 增加用户理财资产
     *
     * @param id
     * @param amount
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set net_asset = net_asset + #{amount}, updated_at = #{updateTime} where id = #{id} and org_id = #{orgId}")
    int updateAssetSnapshot(@Param("orgId") Long orgId, @Param("id") Long id, @Param("amount") BigDecimal amount, @Param("updateTime") Long updateTime);

    /**
     * 更新当日有资产变化的用户资产
     *
     * @param assetSnapshot
     * @return
     */
    @Update(" update " + TABLE_NAME + " set net_asset = net_asset + #{assetSnapshot.netAsset}, pay_interest = #{assetSnapshot.payInterest}, updated_at = #{assetSnapshot.updatedAt}, version_lock = #{assetSnapshot.updatedAt}  where id = #{assetSnapshot.id} and org_id = #{assetSnapshot.orgId} and version_lock is null")
    int updateChangeAssetSnapshot(@Param("assetSnapshot") StakingAssetSnapshot assetSnapshot);

    /**
     * 获取前天的记录
     *
     * @param orgId           机构ID
     * @param productId       理财产品ID
     * @param productRebateId 派息ID
     * @param id              每日资产快照ID
     * @param limit           每次读取记录条数
     * @return
     */
    @Select("select " + FIELDS + " from " + TABLE_NAME + " t1 where t1.org_id = #{orgId} and t1.product_id=#{productId} and t1.product_rebate_id=#{productRebateId} and t1.id > #{id} and t1.net_asset !=0 "
            + " and not exists(select t2.id from tb_staking_asset_snapshot t2 where t1.org_id=t2.org_id and t1.product_id=t2.product_id and t1.user_id=t2.user_id and t2.product_rebate_id=#{curProductRebateId} and t2.version_lock is not null) "
            + " order by t1.id limit #{limit}")
    List<StakingAssetSnapshot> listPreDateData(@Param("orgId") Long orgId, @Param("productId") Long productId
            , @Param("productRebateId") Long productRebateId, @Param("curProductRebateId") Long curProductRebateId
            , @Param("id") Long id, @Param("limit") Integer limit);

    /**
     * 赎回:减少用户资产
     *
     * @param orgId
     * @param productId
     * @param userId
     * @param dailyDate
     * @param amount
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set net_asset = net_asset - #{amount}, updated_at = #{updateTime} where org_id = #{orgId} and product_id = #{productId} and user_id = #{userId} and daily_date = #{dailyDate}")
    int updateReduceAssetSnapshot(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("userId") Long userId, @Param("dailyDate") Long dailyDate, @Param("amount") BigDecimal amount, @Param("updateTime") Long updateTime);

    /**
     * 新增或者更新用户当日资产快照
     * (暂时没有用到)
     *
     * @param assetSnapshotList
     * @param orgId
     * @param userId
     * @param productId
     * @param rebateId
     * @param updateAt
     * @return
     */
    @UpdateProvider(type = StakingProductRebateDetailSqlProvider.class, method = "batchUpdateAssetSnapshot")
    int batchUpdate(List<StakingAssetSnapshot> assetSnapshotList
            , @Param("orgId") Long orgId, @Param("userId") Long userId, @Param("productId") Long productId
            , @Param("rebateId") Long rebateId, @Param("updateAt") Long updateAt);

    /**
     * 获取最新的用户资产快照
     *
     * @param orgId           机构ID
     * @param productId       理财产品ID
     * @param userId          用户ID
     * @return
     */
    @Select("select " + FIELDS + " from " + TABLE_NAME + " t1 where t1.org_id = #{orgId} and t1.product_id=#{productId} and t1.user_id=#{userId} "
            + " order by t1.daily_date desc limit 1")
    StakingAssetSnapshot getLastAssetSnapshot(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("userId") Long userId);
}
