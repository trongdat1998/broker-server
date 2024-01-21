package io.bhex.broker.server.primary.mapper;

import io.bhex.base.margin.Margin;
import io.bhex.broker.server.model.MarginOpenActivity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-03-06 16:29
 */
@Mapper
public interface MarginOpenActivityMapper extends tk.mybatis.mapper.common.Mapper<MarginOpenActivity> {
    @Select("select * from tb_margin_open_activity where org_id = #{orgId} AND account_id = #{accountId}")
    MarginOpenActivity getMarginOpenActivityByAid(@Param("orgId")Long orgId,@Param("accountId")Long accountId);

    @Select("select * from tb_margin_open_activity where org_id = #{orgId} and submit_time >= #{beginTime} and submit_time <= #{endTime} and join_status = 1")
    List<MarginOpenActivity> queryMarginOpenActivityByTime(@Param("orgId")Long orgId,@Param("beginTime") Long beginTime,@Param("endTime") Long endTime);

    @Select("select * from tb_margin_open_activity where org_id = #{orgId} and join_status = 2")
    List<MarginOpenActivity> queryJoinDayMarginOpenActivity(@Param("orgId")Long orgId);

}
