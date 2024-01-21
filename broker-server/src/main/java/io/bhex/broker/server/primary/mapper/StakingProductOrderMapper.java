package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.domain.staking.UserCurrentOrderSum;
import io.bhex.broker.server.model.staking.StakingProductOrder;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

/**
 * 理财申购自定义Mapper
 *
 * @author songxd
 * @date 2020-07-31
 */
public interface StakingProductOrderMapper extends tk.mybatis.mapper.common.Mapper<StakingProductOrder> {

    String TABLE_NAME = "tb_staking_product_order";

    /**
     * 查询理财产品申购流水号是否已经存在
     *
     * @param orgId
     * @param userId
     * @param productId
     * @param transferId
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} and transfer_id = #{transferId}")
    StakingProductOrder getByTransferId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("productId") Long productId, @Param("transferId") Long transferId);

    /**
     * 更新理财产品申购订单状态
     *
     * @param orgId
     * @param id
     * @param status
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set status = #{status}, updated_at = #{updateTime} where id = #{id} and org_id = #{orgId} and status= #{orderStatus}")
    int updateStakingProductOrderStatus(@Param("orgId") Long orgId, @Param("id") Long id, @Param("status") Integer status
            , @Param("orderStatus") Integer orderStatus, @Param("updateTime") Long updateTime);

    /**
     * 更新理财产品申购订单状态为成功
     *
     * @param orgId
     * @param id
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set status = 1, updated_at = #{updateTime} where id = #{id} and org_id = #{orgId} and status = #{orderStatus}; ")
    int updateStakingProductOrderSuccess(@Param("orgId") Long orgId, @Param("id") Long id, @Param("orderStatus") Integer orderStatus, @Param("updateTime") Long updateTime);

    /**
     * 更新理财产品申购订单状态
     *
     * @param id
     * @param redemptionDate
     * @param status
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set status = #{status}, redemption_date = #{redemptionDate}, updated_at = #{updateTime} where id = #{id} and org_id = #{orgId} ")
    int updateStakingProductRedeemStatusAndDate(@Param("orgId") Long orgId, @Param("id") Long id, @Param("status") Integer status
            , @Param("redemptionDate") Long redemptionDate
            , @Param("updateTime") Long updateTime);

    /**
     * 更新理财产品申购订单派息日期
     *
     * @param orgId            机构id
     * @param id
     * @param lastInterestDate
     * @param updateTime
     * @return
     */
    @Update(" update " + TABLE_NAME + " set last_interest_date = #{lastInterestDate}, updated_at = #{updateTime} where id = #{id} and org_id = #{orgId}")
    int updateStakingProductOrderLastInterestDate(@Param("orgId") Long orgId, @Param("id") Long id, @Param("lastInterestDate") Long lastInterestDate
            , @Param("updateTime") Long updateTime);

    /**
     * 获取理财产品的申购记录，参与派息计算
     *
     * @param orgId
     * @param productId
     * @param productRebateId
     * @param limit
     * @return
     */
    @Select("select * from " + TABLE_NAME + " po where po.org_id = #{orgId} and po.product_id = #{productId} and po.order_type=0 and po.status = 1"
            + " and not exists ( "
            + "select id from tb_staking_product_rebate_detail pod "
            + "where pod.org_id = po.org_id and pod.user_id = po.user_id and pod.product_id = po.product_id and pod.order_id = po.id "
            + "and pod.product_rebate_id = #{productRebateId} and pod.status in (0,1)) limit #{limit} ")
    List<StakingProductOrder> listProductRebateOrder(@Param("orgId") Long orgId, @Param("productId") Long productId
            , @Param("productRebateId") Long productRebateId, @Param("limit") Integer limit);

    /**
     * 获取理财产品的申购记录，参与派息计算
     *
     * @param orgId
     * @param productId
     * @param productRebateId
     * @param limit
     * @return
     */
    @Select("select * from " + TABLE_NAME + " po where po.org_id = #{orgId} and po.product_id = #{productId} and po.order_type=0 and po.status in (1,4) "
            + " and not exists ( "
            + "select id from tb_staking_product_rebate_detail pod "
            + "where pod.org_id = po.org_id and pod.user_id = po.user_id and pod.product_id = po.product_id and pod.order_id = po.id "
            + "and pod.product_rebate_id = #{productRebateId} and pod.status in (0,1)) limit #{limit} ")
    List<StakingProductOrder> listReCalcProductOrders(@Param("orgId") Long orgId, @Param("productId") Long productId
            , @Param("productRebateId") Long productRebateId, @Param("limit") Integer limit);

    /**
     * 获取用户理财活期合计金额
     *
     * @param orgId
     * @param productId
     * @param userId
     * @param startDate
     * @param endDate
     * @param productRebateId
     * @param limit
     * @return
     */
    @Select("select t1.org_id,t1.product_id,t1.user_id,sum(t1.pay_amount) amount " +
            "from tb_staking_product_order t1 " +
            "where t1.org_id = #{orgId} and t1.product_id = #{productId} and t1.status = 1 and product_type = 1 and t1.user_id > #{userId} and t1.order_type in (0,1) and t1.created_at >= #{startDate} and t1.created_at < #{endDate} "
            + " and not exists( select t2.id from tb_staking_asset_snapshot t2 where t2.org_id=t1.org_id and t2.product_id=t1.product_id and t1.user_id=t2.user_id and t2.product_rebate_id=#{productRebateId} limit 1)"
            + " group by t1.org_id,t1.product_id,t1.user_id order by t1.user_id limit #{limit}; ")
    List<UserCurrentOrderSum> listUserCurrentOrderSum(@Param("orgId") Long orgId, @Param("productId") Long productId
            , @Param("userId") Long userId, @Param("startDate") Long startDate, @Param("endDate") Long endDate
            , @Param("productRebateId") Long productRebateId, @Param("limit") Integer limit);

    @Select("select * from tb_staking_product_order where org_id = #{orgId} and product_id = #{productId} and status in (1,3,4) and (#{userId} = 0 or user_id = #{userId}) and (#{orderId} = 0 or id = #{orderId}) and (#{startId} = 0 or id < #{startId}) order by id desc limit #{limit};")
    List<StakingProductOrder> queryBrokerProductOrder(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("userId") Long userId, @Param("orderId") Long orderId, @Param("startId") Long startId, @Param("limit") Integer limit);

    @Select("select * from tb_staking_product_order where org_id = #{orgId} and id = #{orderId}")
    StakingProductOrder getOrderById(@Param("orgId") Long orgId, @Param("orderId") Long orderId);

    /**
     * 获取理财产品订单总金额
     *
     * @param orgId
     * @param productId
     * @param userId
     * @param lastDate
     * @return
     */
    @Select("select sum(pay_amount) from tb_staking_product_order where org_id=#{orgId} and product_id=#{productId} and user_id=#{userId} and order_type in (1,2) and created_at>=#{lastDate} and status IN (0,1);")
    BigDecimal sumRedeemOrderAmountByUserId(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("userId") Long userId, @Param("lastDate") Long lastDate);

    /**
     * 获取理财产品订单总金额
     *
     * @param orgId
     * @param productId
     * @param userId
     * @param lastDate
     * @return
     */
    @Select("select sum(pay_amount) from tb_staking_product_order where org_id=#{orgId} and product_id=#{productId} and user_id=#{userId} and order_type=0 and created_at>=#{lastDate} and status=1;")
    BigDecimal sumOrderAmountByUserId(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("userId") Long userId, @Param("lastDate") Long lastDate);

    /**
     * 获取理财产品订单总金额
     *
     * @param orgId
     * @param productId
     * @param userId
     * @return
     */
    @Select("select sum(pay_amount) from tb_staking_product_order where org_id=#{orgId} and product_id=#{productId} and user_id=#{userId} and order_type=0 and status=1;")
    BigDecimal sumOrderAmountByUserIdAndProduct(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("userId") Long userId);

    /**
     * 获取未处理订单数量
     *
     * @param orgId
     * @param productId
     * @return
     */
    @Select("select count(*) from tb_staking_product_order where org_id=#{orgId} and product_id=#{productId} and status=0 ")
    Integer countProcessingOfOrder(@Param("orgId") Long orgId, @Param("productId") Long productId);

    /**
     * 获取活期赎回还本订单
     *
     * @param orgId
     * @param productId
     * @param startId
     * @param limit
     * @return
     */
    @Select("select * from tb_staking_product_order where org_id = #{orgId} and product_id = #{productId} and status=0 and order_type=2 and id>#{startId} order by id limit #{limit};")
    List<StakingProductOrder> listCurrentRedeemPrincipalOrders(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("startId") Long startId, @Param("limit") Integer limit);
}
