package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterest;
import io.bhex.broker.server.model.ActivityPurchaseLimit;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.primary.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/7/23 11:52 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityPurchaseLimitMapper extends Mapper<ActivityPurchaseLimit> {

    String TABLE_NAME = " tb_activity_purchase_limit ";

    String COLUMNS = " * ";

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where project_id = #{projectId}")
    ActivityPurchaseLimit getLimitById(@Param("projectId") Long projectId);

}
