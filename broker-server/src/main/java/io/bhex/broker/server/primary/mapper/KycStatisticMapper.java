package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.KycStatistic;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@Component
@org.apache.ibatis.annotations.Mapper
public interface KycStatisticMapper extends Mapper<KycStatistic> {


    @Select("select * from tb_kyc_statistic where  org_id = #{orgId} and statistic_date = #{statisticDate}  and aggregate = 0")
    KycStatistic getDailyKycStatisticByDate(@Param("orgId") Long orgId, @Param("statisticDate") String statisticDate);


    @Select("select * from tb_kyc_statistic where  org_id = #{orgId} and aggregate = 1")
    KycStatistic getAggregateKycStatistic(@Param("orgId") Long orgId);


    @Select("select max(last_verify_history_id) from tb_kyc_statistic")
    Long getLastVerifyHistoryId();

    @Select("select * from tb_kyc_statistic where  org_id = #{orgId} and statistic_date between #{startDate} and #{endDate}"
            + " and aggregate = 0 order by statistic_date desc")
    List<KycStatistic> getDailyKycStatistics(@Param("orgId") Long orgId, @Param("startDate") String startDate, @Param("endDate") String endDate);

}
