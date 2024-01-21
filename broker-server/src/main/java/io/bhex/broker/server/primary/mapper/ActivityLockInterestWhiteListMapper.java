package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterestWhiteList;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;


@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestWhiteListMapper extends Mapper<ActivityLockInterestWhiteList> {

    @Select("select *from tb_activity_lock_interest_white_list where broker_id = #{brokerId} and project_id = #{projectId} limit 0,1")
    ActivityLockInterestWhiteList queryByProjectId(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Update("update tb_activity_lock_interest_white_list set user_id_str = #{userIdStr} where broker_id=#{brokerId} and project_id = #{projectId}")
    void updateUserStrByProjectId(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId, @Param("userIdStr") String userIdStr);
}