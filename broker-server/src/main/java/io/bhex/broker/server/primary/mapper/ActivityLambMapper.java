package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLamb;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/5/29 11:43 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLambMapper extends Mapper<ActivityLamb> {

    String TABLE_NAME = " tb_activity_lamb ";

    String COLUMNS = " * ";

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where id = #{projectId} for update")
    ActivityLamb getByIdWithLock(@Param("projectId") Long projectId);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where id = #{projectId}")
    ActivityLamb getActivityLockInterestById(@Param("projectId") Long projectId);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} order by created_time desc")
    List<ActivityLamb> listProjectInfo(@Param("brokerId") Long brokerId);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where id = #{projectId} and broker_id = #{brokerId}")
    ActivityLamb getActivityByProjectId(@Param("projectId") Long projectId, @Param("brokerId") Long brokerId);
}
