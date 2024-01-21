package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

import io.bhex.broker.server.model.ActivityLockInterestBatchDetail;
import tk.mybatis.mapper.common.special.InsertListMapper;

@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestBatchDetailMapper extends tk.mybatis.mapper.common.Mapper<ActivityLockInterestBatchDetail>, InsertListMapper<ActivityLockInterestBatchDetail> {

    @Update("update tb_activity_lock_interest_batch_detail set status = 1 where org_id = #{orgId} and project_id = #{projectId} and task_id = #{taskId}")
    int updateStatusToSuccess(@Param("orgId") Long orgId, @Param("projectId") Long projectId, @Param("taskId") Long taskId);

    @Update("update tb_activity_lock_interest_batch_detail set status = 2 where org_id = #{orgId} and project_id = #{projectId} and task_id = #{taskId}")
    int updateStatusToFail(@Param("orgId") Long orgId, @Param("projectId") Long projectId, @Param("taskId") Long taskId);
}
