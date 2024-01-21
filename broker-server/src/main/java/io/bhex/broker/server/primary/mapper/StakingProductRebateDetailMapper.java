package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.grpc.server.service.staking.StakingTransferEvent;
import io.bhex.broker.server.model.staking.StakingProductRebateDetail;
import io.bhex.broker.server.model.staking.StakingProductRebateDetailTotal;
import org.apache.ibatis.annotations.*;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface StakingProductRebateDetailMapper extends tk.mybatis.mapper.common.Mapper<StakingProductRebateDetail> {

    /**
     * 获取要进行派息转账的理财记录
     *
     * @param orgId
     * @param productId
     * @param productType
     * @param productRebateId
     * @return
     */
    @Select("SELECT * FROM tb_staking_product_rebate_detail where org_id = #{orgId} and product_id = #{productId} and product_type = #{productType} and product_rebate_id=#{productRebateId} and status = 0 limit 100 ")
    List<StakingProductRebateDetail> getWatingTransferList(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("productType") Integer productType, @Param("productRebateId") Long productRebateId);

    /**
     * 获取派息记录要派息的合计金额
     *
     * @param transferEvent
     * @return
     */
    @SelectProvider(type = StakingProductRebateDetailSqlProvider.class, method = "selectRebateTotal")
    List<StakingProductRebateDetailTotal> listStakingProductRebateDetailTotalById(StakingTransferEvent transferEvent);

    /**
     * 更新理财派息转账状态
     *
     * @param orgId      机构id
     * @param id         理财派息记录id
     * @param status     状态
     * @param updateTime 时间戳
     * @return 更新结果
     */
    @Update(" update tb_staking_product_rebate_detail set status = #{status}, updated_at = #{updateTime} where id = #{id} and org_id = #{orgId} ")
    int updateStakingRebateDetailStatus(@Param("orgId") Long orgId, @Param("id") Long id, @Param("status") Integer status, @Param("updateTime") Long updateTime);

    /**
     * 获取当前派息设置的合计派息金额
     *
     * @param orgId
     * @param productId
     * @param productRebateId
     * @return
     */
    @Select("select sum(t1.rebate_amount) from tb_staking_product_rebate_detail t1 where org_id = #{orgId} and product_id = #{productId} and product_rebate_id = #{productRebateId} and rebate_type=0 ")
    BigDecimal sumRebateAmount(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("productRebateId") Long productRebateId);

    /**
     * 获取当前派息设置的合计派息金额
     *
     * @param orgId
     * @param productId
     * @param productRebateId
     * @return
     */
    @Select("select sum(t1.rebate_amount) from tb_staking_product_rebate_detail t1 where org_id = #{orgId} and product_id = #{productId} and product_rebate_id = #{productRebateId} and rebate_type=1 ")
    BigDecimal sumPrincipalRebateAmount(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("productRebateId") Long productRebateId);

    /**
     * 获取指定产品的有效还本记录
     *
     * @param orgId
     * @param productId
     * @param productType
     * @return
     */
    @Select("SELECT * FROM tb_staking_product_rebate_detail where org_id = #{orgId} and product_id = #{productId} and product_type = #{productType} and rebate_type=1 and status in (0,1) limit 10 ")
    List<StakingProductRebateDetail> getPrincipalList(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("productType") Integer productType);

    /**
     * 获取未执行转账的记录数
     *
     * @param orgId
     * @param productId
     * @return
     */
    @Select("select count(*) from tb_staking_product_rebate_detail t1 where org_id = #{orgId} and product_id = #{productId} and status=0 ")
    Integer countNoTransfer(@Param("orgId") Long orgId, @Param("productId") Long productId);

    /**
     * 获取派息配置已派息的记录数
     *
     * @param orgId
     * @param productId
     * @param productRebateId
     * @return
     */
    @Select("select count(*) from tb_staking_product_rebate_detail t1 where org_id = #{orgId} and product_id = #{productId} and product_rebate_id = #{productRebateId} and status=1 ")
    Integer countTransferByRebateId(@Param("orgId") Long orgId, @Param("productId") Long productId
            , @Param("productRebateId") Long productRebateId);
}
