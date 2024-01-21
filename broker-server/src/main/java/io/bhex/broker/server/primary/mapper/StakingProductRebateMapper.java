package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.domain.staking.StakingRebateAvailableCount;
import io.bhex.broker.server.model.staking.StakingProductRebate;
import org.apache.ibatis.annotations.*;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface StakingProductRebateMapper extends tk.mybatis.mapper.common.Mapper<StakingProductRebate> {
    String TABLE_NAME = "tb_staking_product_rebate";

    /**
     * 获取理财(定期+锁仓)派息设置
     *
     * @param orgId      机构ID
     * @param rebateDate 派息日期
     * @param status     状态
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and rebate_date <= #{rebateDate} and status = #{status} AND product_type in (0,2) and type = 0 order by rebate_date,product_id")
    List<StakingProductRebate> listCalcInterestTask(@Param("orgId") Long orgId, @Param("rebateDate") Long rebateDate, @Param("status") Integer status);

    /**
     * 获取活期理财派息设置
     *
     * @param orgId        机构ID
     * @param dividendDate 派息日期
     * @param status       状态
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and status = #{status} and rebate_date < #{dividendDate} and product_type = 1 and type = 0 order by rebate_date,product_id")
    List<StakingProductRebate> listCurrentCalcInterestTask(@Param("orgId") Long orgId, @Param("dividendDate") Long dividendDate, @Param("status") Integer status);

    /**
     * 获取活期理财还本设置
     *
     * @param orgId        机构ID
     * @param dividendDate 派息日期
     * @param status       状态
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and status = #{status} and rebate_date < #{dividendDate} and product_type = 1 and type = 1 order by rebate_date,product_id")
    List<StakingProductRebate> listCurrentPrincipal(@Param("orgId") Long orgId, @Param("dividendDate") Long dividendDate, @Param("status") Integer status);

    /**
     * 获取活期理财派息设置
     *
     * @param orgId 机构ID
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and status = 1 and product_type = 1 and type = 0 order by rebate_date,product_id")
    List<StakingProductRebate> listWaitTransfer(@Param("orgId") Long orgId);

    /**
     * 判断是否是最后一次派息(定期+锁仓)
     *
     * @param orgId           机构ID
     * @param productId       理财产品ID
     * @param productRebateId 派息设置ID
     * @param productType     理财产品类型
     * @return 返回还未派息的派息记录
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and product_id = #{productId} and status = 0 and product_type = #{productType} and type = 0 AND id <> #{productRebateId} limit 1")
    StakingProductRebate checkIsLastInterest(@Param("orgId") Long orgId
            , @Param("productId") Long productId
            , @Param("productRebateId") Long productRebateId
            , @Param("productType") Integer productType);

    /**
     * 获取上一次已派息记录
     *
     * @param orgId           机构ID
     * @param productId       理财产品ID
     * @param productRebateId 派息设置ID
     * @param productType     理财产品类型
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and product_id = #{productId} and status in (1,2) and rebate_date<#{rebateDate} and product_type = #{productType} and type = 0 AND id != #{productRebateId} order by rebate_date desc limit 1")
    StakingProductRebate getPreStakingProductRebate(@Param("orgId") Long orgId
            , @Param("productId") Long productId
            , @Param("productRebateId") Long productRebateId
            , @Param("productType") Integer productType
            , @Param("rebateDate") Long rebateDate);

    /**
     * 获取本金派息配置
     *
     * @param orgId     机构ID
     * @param productId 理财产品ID
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and product_id = #{productId} and type = 1 and status = 0")
    StakingProductRebate getPrincipalRebate(@Param("orgId") Long orgId
            , @Param("productId") Long productId);

    /**
     * 获取最后一条有效派息配置
     *
     * @param orgId     机构ID
     * @param productId 理财产品ID
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and product_id = #{productId} and type = 0 and product_type=1 and status = 0 order by rebate_date desc limit 1")
    StakingProductRebate getLastAvailableRebate(@Param("orgId") Long orgId, @Param("productId") Long productId);


    /**
     * 更新派息设置状态
     *
     * @param orgId      机构ID
     * @param id         派息设置ID
     * @param status     状态
     * @param updateTime 时间戳
     * @return 更新记录数
     */
    @Update("update " + TABLE_NAME + " set status = #{status},updated_at = #{updateTime} where id = #{id} and org_id = #{orgId}")
    int updateStatus(@Param("orgId") Long orgId, @Param("id") Long id, @Param("status") Integer status, @Param("updateTime") Long updateTime);

    /**
     * 更新派息设置状态
     *
     * @param orgId      机构ID
     * @param id         派息设置ID
     * @param updateTime 时间戳
     * @return 更新记录数
     */
    @Update("update " + TABLE_NAME + " set status = 4,updated_at = #{updateTime} where id = #{id} and org_id = #{orgId} and status=0")
    int updateInvalidStatus(@Param("orgId") Long orgId, @Param("id") Long id, @Param("updateTime") Long updateTime);

    /**
     * 更新派息设置状态
     *
     * @param productRebate 派息设置
     * @return 更新记录数
     */
    @UpdateProvider(type = StakingProductRebateSqlProvider.class, method = "updateStatusAndAmountOrRate")
    int updateStatusAndAmountOrRate(StakingProductRebate productRebate);

    /**
     * 更新派息记录，需派息时间大于传入的时间条件且状态为0（待派息）
     *
     * @param id
     * @param orgId
     * @param productId
     * @param conditionRebateDate
     * @param updateRebateDate
     * @param updateRebateRate
     * @param updateTime
     * @return
     */
    @Update("update " + TABLE_NAME + " set rebate_date = #{updateRebateDate}, token_id = #{updateTokenId},rebate_rate = #{updateRebateRate},rebate_amount = #{updateRebateAmount},updated_at = #{updateTime} where id = #{id} and org_id = #{orgId} and product_id = #{productId} and rebate_date > #{conditionRebateDate} and status = 0;")
    int updateRebate(@Param("id") Long id,
                     @Param("orgId") Long orgId,
                     @Param("productId") Long productId,
                     @Param("conditionRebateDate") Long conditionRebateDate,
                     @Param("updateRebateDate") Long updateRebateDate,
                     @Param("updateTokenId") String updateTokenId,
                     @Param("updateRebateRate") BigDecimal updateRebateRate,
                     @Param("updateRebateAmount") BigDecimal updateRebateAmount,
                     @Param("updateTime") Long updateTime);

    /**
     * 批量更新活期派息记录的利率，需派息时间大于传入的时间条件且状态为0（待派息）
     *
     * @param orgId
     * @param productId
     * @param conditionRebateDate
     * @param updateRebateRate
     * @param updateTime
     * @return
     */
    @Update("update " + TABLE_NAME + " set rebate_rate = #{updateRebateRate}, updated_at = #{updateTime} where org_id = #{orgId} and product_id = #{productId} and rebate_date > #{conditionRebateDate} and status = 0;")
    int updateCurrentRebate(@Param("orgId") Long orgId,
                            @Param("productId") Long productId,
                            @Param("conditionRebateDate") Long conditionRebateDate,
                            @Param("updateRebateRate") BigDecimal updateRebateRate,
                            @Param("updateTime") Long updateTime);

    /**
     * 批量更新活期派息记录的状态，需派息时间大于传入的时间条件且状态为0（待派息）
     *
     * @param orgId
     * @param productId
     * @param conditionRebateDate
     * @param updateTime
     * @param status
     * @return
     */
    @Update("update " + TABLE_NAME + " set status = #{status}, updated_at = #{updateTime} where org_id = #{orgId} and product_id = #{productId} and rebate_date > #{conditionRebateDate} and status = 0;")
    int updateCurrentRebateStatusByCondition(@Param("orgId") Long orgId,
                                             @Param("productId") Long productId,
                                             @Param("conditionRebateDate") Long conditionRebateDate,
                                             @Param("updateTime") Long updateTime,
                                             @Param("status") Integer status);

    /**
     * 获取最后一条非无效派息记录
     * @param orgId
     * @param productId
     * @return
     */
    @Select("select * from " + TABLE_NAME + " where org_id = #{orgId} and product_id = #{productId} and type = 0 and status != 4 order by rebate_date desc limit 1;")
    StakingProductRebate getLastInterestRebate(@Param("orgId") Long orgId,
                                               @Param("productId") Long productId);


    @Update("update " + TABLE_NAME + " set status = #{status},updated_at = #{updateTime} where org_id = #{orgId} and product_id = #{productId} and status = 0;")
    int updateRebateStatusById(@Param("orgId") Long orgId,
                               @Param("productId") Long productId,
                               @Param("status") Integer status,
                               @Param("updateTime") Long updateTime);

    /**
     * 更新本金派息设置记录
     *
     * @param orgId
     * @param productId
     * @param amount
     * @return
     */
    @Update("update tb_staking_product_rebate set rebate_amount=#{amount},status=1,updated_at = #{updateTime} where org_id=#{orgId} and product_id=#{productId} and id = #{rebateId} and status=0")
    int updatePrincipalRebate(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("rebateId") Long rebateId, @Param("amount") BigDecimal amount, @Param("updateTime") Long updateTime);


    /**
     * 获取理财产品为派息的设置
     *
     * @param orgId  机构ID
     * @param status 状态
     * @return
     */
    @Select("select product_id as productId,count(1) availableCount from tb_staking_product_rebate where org_id=#{orgId} and product_type=1 and type=0 and status=#{status} group by product_id")
    List<StakingRebateAvailableCount> listCurrentAvailableRebateCount(@Param("orgId") Long orgId, @Param("status") Integer status);
}
