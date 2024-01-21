package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

import io.bhex.broker.server.model.TradeCompetitionResultDaily;


@Mapper
public interface TradeCompetitionResultDailyMapper extends tk.mybatis.mapper.common.Mapper<TradeCompetitionResultDaily> {

    @Select("select user_id,rate,income_amount from tb_trade_competition_result_daily where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} and status = 1 and rate between -1 and 6 order by rate desc limit #{limit}")
    List<TradeCompetitionResultDaily> queryRateListByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("limit") Integer limit);


    @Select("select user_id,rate,income_amount from tb_trade_competition_result_daily where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} order by rate desc limit #{limit}")
    List<TradeCompetitionResultDaily> queryRateListByCodeV2(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("limit") Integer limit);

//    @InsertProvider(type = CompetitionSqlProvider.class, method = "saveTradeCompetitionResult")
//    int saveTradeCompetitionResult(@Param("list") List<TradeCompetitionResult> list);

    @Select("select max(batch) from tb_trade_competition_result_daily where org_id=#{orgId} and competition_code = #{competitionCode} and day=#{day}")
    String queryMaxBatchByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("day") String day);

    @Delete("delete from tb_trade_competition_result_daily where org_id= #{orgId} and competition_code = #{competitionCode} and day=#{day} and batch < #{batch}")
    int clearTradeCompetitionResult(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("day") String day);

    @Select("select *from tb_trade_competition_result_daily where org_id = #{orgId} and competition_code  = #{competitionCode} and transfer_status = 0")
    List<TradeCompetitionResultDaily> queryWaitingTradeCompetitionList(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode);

    @Update("update tb_trade_competition_result_daily set transfer_status = 1 where id = #{id}")
    int updateTradeCompetitionStatus(@Param("id") Long id);

    @Select("select * from tb_trade_competition_result_daily where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} and user_id = #{userId} and day=#{day}")
    TradeCompetitionResultDaily queryByUserIdAndBatch(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("userId") Long userId, @Param("batch") String batch, @Param("day") String day);

    @Select("select user_id,rate,income_amount from tb_trade_competition_result_daily where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} and status = 1 order by income_amount desc limit #{limit}")
    List<TradeCompetitionResultDaily> listIncomeAmountByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("limit") Integer limit);

    @Select("select user_id,rate,income_amount from tb_trade_competition_result_daily where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} order by income_amount desc limit #{limit}")
    List<TradeCompetitionResultDaily> listIncomeAmountByCodeV2(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("limit") Integer limit);

    @Select("select user_id,rate,income_amount from tb_trade_competition_result_daily where org_id=#{orgId} and competition_code = #{competitionCode} and day=#{day} and batch = #{batch} and user_id =#{userId}")
    TradeCompetitionResultDaily queryUserPerformanceByDay(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("userId") Long userId, @Param("day") String day);

    @Select(" select day from tb_trade_competition_result_daily where org_id=#{orgId} and competition_id=#{competitionId} group by day desc ")
    List<String> listDay(@Param("orgId") Long orgId, @Param("competitionId") Long competitionId);
}

