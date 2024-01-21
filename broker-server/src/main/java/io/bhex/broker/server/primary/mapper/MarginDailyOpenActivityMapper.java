package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MarginDailyOpenActivity;
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
public interface MarginDailyOpenActivityMapper extends tk.mybatis.mapper.common.Mapper<MarginDailyOpenActivity> {

    @Select("select * from tb_margin_daily_open_activity where org_id = #{orgId} AND submit_time = #{submitTime} AND account_id = #{accountId}")
    MarginDailyOpenActivity getMarginOpenActivityByAid(@Param("orgId") Long orgId, @Param("submitTime") Long submitTime, @Param("accountId") Long accountId);

    @Select("select IFNULL(MAX(lottery_no),0) from tb_margin_daily_open_activity where org_id = #{orgId} and submit_time >= #{beginTime} and submit_time < #{endTime}")
    Integer getMaxLotteryNo(@Param("orgId") Long orgId, @Param("beginTime") Long beginTime,@Param("endTime")Long endTime);

    @Select("select * from tb_margin_daily_open_activity where org_id = #{orgId} and submit_time >= #{beginTime} and submit_time < #{endTime} AND account_id = #{accountId} order by id")
    List<MarginDailyOpenActivity> queryMarginOpenActivityByAid(@Param("orgId") Long orgId, @Param("beginTime") Long beginTime,@Param("endTime")Long endTime,@Param("accountId") Long accountId);

}
