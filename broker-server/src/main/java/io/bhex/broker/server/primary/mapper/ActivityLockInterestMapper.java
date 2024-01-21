package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterest;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/6/4 3:06 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestMapper extends Mapper<ActivityLockInterest> {

    String TABLE_NAME = " tb_activity_lock_interest ";

    String COLUMNS = " * ";

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where id = #{projectId} for update")
    ActivityLockInterest getByIdWithLock(@Param("projectId") Long projectId);


    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where id = #{projectId}")
    ActivityLockInterest getByIdNoLock(@Param("projectId") Long projectId);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} and project_code = #{projectCode} order by created_time desc limit 1")
    ActivityLockInterest getByCodeNoLock(@Param("brokerId") Long brokerId, @Param("projectCode") String projectCode);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} and project_code = #{projectCode} order by created_time desc")
    List<ActivityLockInterest> listProjectInfo(@Param("brokerId") Long brokerId, @Param("projectCode") String projectCode);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where id = #{projectId}")
    ActivityLockInterest getActivityLockInterestById(@Param("projectId") Long projectId);

    @SelectProvider(type = ActivityLockInterestProvider.class, method = "orgListActivityByPage")
    List<ActivityLockInterest> orgListActivityByPage(@Param("brokerId") Long brokerId, @Param("fromId") Long fromId, @Param("endId") Long endId, @Param("size") Integer size, @Param("orderDesc") boolean orderDesc);

    @Update("update tb_activity_lock_interest set is_show = #{isShow} where id = #{id}")
    int updateIsShow(@Param("id") Long id, @Param("isShow") Integer isShow);

    @Update("update tb_activity_lock_interest set status = 4 where project_code = #{projectCode}")
    int updateActivityStatusByProjectCode(@Param("projectCode") String projectCode);

}
