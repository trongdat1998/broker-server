package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

import io.bhex.broker.server.model.ActivityLockInterestBatchTask;
import tk.mybatis.mapper.common.Mapper;

@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestBatchTaskMapper extends Mapper<ActivityLockInterestBatchTask> {

    @Update("update tb_activity_lock_interest_batch_task set status = 1 where id = #{id}")
    int updateStatusToSuccess(@Param("id") Long id);

    @Update("update tb_activity_lock_interest_batch_task set status = 2 where id = #{id}")
    int updateStatusToFail(@Param("id") Long id);
}
