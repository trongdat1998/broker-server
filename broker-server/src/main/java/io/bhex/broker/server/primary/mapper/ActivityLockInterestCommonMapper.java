package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterestCommon;
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
 * @CreateDate: 2019/6/5 4:24 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestCommonMapper extends Mapper<ActivityLockInterestCommon> {

    String TABLE_NAME = " tb_activity_lock_interest_common ";

    String COLUMNS = " * ";

    String LIST_COLUMNS = "id, broker_id, project_code, banner_url, language, status, created_time, updated_time, end_time, start_time, activity_type, online_time, result_time,introduction";

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where project_code = #{projectCode} and broker_id = #{brokerId} and language = #{language} limit 1")
    ActivityLockInterestCommon getCommonInfo(@Param("projectCode") String projectCode, @Param("language") String language, @Param("brokerId") Long brokerId);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where project_code = #{projectCode} and broker_id = #{brokerId} limit 1")
    ActivityLockInterestCommon getCommonInfoByCode(@Param("projectCode") String projectCode, @Param("brokerId") Long brokerId);

    @SelectProvider(type = ActivityLockInterestCommonProvider.class, method = "listActivityByPage")
    List<ActivityLockInterestCommon> listActivityByPage(@Param("brokerId") Long brokerId, @Param("fromId") Long fromId,
                                                        @Param("endId") Long endId, @Param("size") Integer size, @Param("language") String language);

    @Update("update tb_activity_lock_interest_common set result_time = #{resultTime} where project_code = #{projectCode}")
    int updateActivityLockInterestCommonResultTime(@Param("resultTime") Long resultTime, @Param("projectCode") String projectCode);

    @Update("update tb_activity_lock_interest_common set status = 4 where project_code = #{projectCode}")
    int updateActivityStatusByProjectCode(@Param("projectCode") String projectCode);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where project_code = #{projectCode} and broker_id = #{brokerId} limit 1")
    ActivityLockInterestCommon getCommonInfoByProjectCode(@Param("projectCode") String projectCode, @Param("brokerId") Long brokerId);
}
