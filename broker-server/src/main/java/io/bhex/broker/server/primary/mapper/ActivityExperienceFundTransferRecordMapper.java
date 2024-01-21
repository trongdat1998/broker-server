package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityExperienceFundTransferRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

@Mapper
public interface ActivityExperienceFundTransferRecordMapper extends
        tk.mybatis.mapper.common.Mapper<ActivityExperienceFundTransferRecord>, InsertListMapper<ActivityExperienceFundTransferRecord> {

    @Select("select user_id from tb_activity_experience_fund_transfer_record where broker_id = #{brokerId} and activity_id = #{activityId}")
    List<Long> getUserIds(@Param("brokerId") Long brokerId, @Param("activityId") Long activityId);



}
