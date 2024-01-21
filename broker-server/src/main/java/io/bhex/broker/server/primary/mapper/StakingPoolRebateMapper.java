package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingPoolRebate;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface StakingPoolRebateMapper extends tk.mybatis.mapper.common.Mapper<StakingPoolRebate> {

    String TABLE_NAME = "tb_staking_pool_rebate";

    /**
     * 获取持仓派息设置
     * @param rebateDate
     * @param stakingType
     * @param status
     * @return
     */
    @Select("SELECT * FROM " + TABLE_NAME + " WHERE status = #{status} and staking_type = #{stakingType} AND rebate_date = #{rebateDate}")
    List<StakingPoolRebate> listCalcRebate(@Param("rebateDate") Long rebateDate, @Param("stakingType") Integer stakingType, @Param("status") Integer status);

    /**
     * 获取充币记录
     * @param orgId
     * @param stakingType
     * @param limit
     * @return
     */
    @Select("SELECT deposit_record_id FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and staking_type = #{stakingType} order by id desc limit #{limit}")
    List<Long> listDepositRecordId(@Param("orgId") Long orgId, @Param("stakingType") Integer stakingType, @Param("limit") Integer limit);

    /**
     * 更新派息设置状态
     * @param id
     * @param status
     * @param updateTime
     * @return
     */
    @Update("update " + TABLE_NAME + " set status = #{status},updated_at = #{updateTime} where id = #{id}")
    int updateStatus(@Param("id") Long id, @Param("status") Integer status, @Param("updateTime") Long updateTime);
}
