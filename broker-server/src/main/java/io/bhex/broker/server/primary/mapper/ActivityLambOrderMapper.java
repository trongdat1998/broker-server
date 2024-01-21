package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLambOrder;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/5/29 11:47 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLambOrderMapper extends Mapper<ActivityLambOrder> {


    @Select("select *from tb_activity_lamb_order where project_id = #{projectId}")
    List<ActivityLambOrder> queryOrderListByProjectId(@Param("projectId")Long projectId);

    @Select("select *from tb_activity_lamb_order where project_id = #{projectId} and status = #{status}")
    List<ActivityLambOrder> queryOrderIdListByStatus(@Param("projectId") Long projectId, @Param("status") Integer status);
}
