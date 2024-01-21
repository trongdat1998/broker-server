package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterestGift;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.primary.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/7/16 3:45 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestGiftMapper extends Mapper<ActivityLockInterestGift> {

    String TABLE_NAME = " tb_activity_lock_interest_gift ";

    String COLUMNS = " * ";

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where project_id = #{projectId} and broker_id = #{brokerId} limit 1")
    ActivityLockInterestGift getGiftInfoByProjectId(@Param("projectId") Long projectId, @Param("brokerId") Long brokerId);
}
