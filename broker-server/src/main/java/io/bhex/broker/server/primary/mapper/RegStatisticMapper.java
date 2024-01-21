package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.RegStatistic;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@Component
@org.apache.ibatis.annotations.Mapper
public interface RegStatisticMapper extends Mapper<RegStatistic> {


    @Select("select * from tb_reg_statistic where  org_id = #{orgId} and statistic_date = #{statisticDate}  and aggregate = 0")
    RegStatistic getDailyRegStatisticByDate(@Param("orgId") Long orgId, @Param("statisticDate") String statisticDate);

    @Select("select * from tb_reg_statistic where org_id = #{orgId} and aggregate = 1")
    RegStatistic getAggregateRegStatistic(@Param("orgId") Long orgId);

    @Select("select max(last_id) from tb_reg_statistic")
    Long getLastId();

    @Select("select * from tb_reg_statistic where  org_id = #{orgId} and statistic_date between #{startDate} and #{endDate}  and aggregate = 0 order by statistic_date desc")
    List<RegStatistic> getDailyRegStatistics(@Param("orgId") Long orgId, @Param("startDate") String startDate, @Param("endDate") String endDate);

}
