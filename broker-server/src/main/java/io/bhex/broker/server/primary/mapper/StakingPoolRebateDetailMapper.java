package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingPoolRebateDetail;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface StakingPoolRebateDetailMapper extends tk.mybatis.mapper.common.Mapper<StakingPoolRebateDetail> {
    /**
     * 获取要进行派息的记录
     * @param orgId
     * @param stakingId
     * @param stakingRebateId
     * @return
     */
    @Select("SELECT * FROM tb_staking_pool_rebate_detail where org_id = #{orgId} and staking_id = #{stakingId} and staking_rebate_id=#{stakingRebateId} and status = 0 limit 100 ")
    List<StakingPoolRebateDetail> getWatingTransferList(@Param("orgId") Long orgId, @Param("stakingId") Long stakingId
            , @Param("stakingRebateId") Long stakingRebateId);

    /**
     * 更新派息记录状态
     * @param orgId
     * @param stakingRebateId
     * @param stakingId
     * @param status
     * @param updateTime
     * @return
     */
    @Update("update tb_staking_pool_rebate_detail set status = #{status},updated_at = #{updateTime} where org_id = #{orgId} and staking_rebate_id = #{stakingRebateId} and staking_id = #{stakingId}")
    int batchUpdateStatusByStakingRebateId(@Param("orgId") Long orgId, @Param("stakingRebateId") Long stakingRebateId
            , @Param("stakingId") Long stakingId
            , @Param("status") Integer status
            , @Param("updateTime") Long updateTime);


    /**
     * 更新派息记录状态
     * @param id
     * @param updateTime
     * @param status
     * @return
     */
    @Update("update tb_staking_pool_rebate_detail set status = #{status},updated_at = #{updateTime} where id = #{id}")
    int updateStatus(@Param("orgId") Long id, @Param("updateTime") Long updateTime, @Param("status") Integer status);
}
