package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MarginDailyRiskStatistics;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-01-11 10:07
 */
@Mapper
public interface MarginDailyRiskStatisticsMapper extends tk.mybatis.mapper.common.Mapper<MarginDailyRiskStatistics> {

    @Select("select * from tb_margin_daily_risk_statistics where org_id = #{orgId} and date = #{date};")
    MarginDailyRiskStatistics getByOrgIdAndDate(@Param("orgId")Long orgId,@Param("date")Long date);
}
