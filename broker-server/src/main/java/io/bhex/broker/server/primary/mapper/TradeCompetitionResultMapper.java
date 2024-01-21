package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.TradeCompetitionResult;
import org.apache.ibatis.annotations.*;

import java.util.List;


@Mapper
public interface TradeCompetitionResultMapper extends tk.mybatis.mapper.common.Mapper<TradeCompetitionResult> {

    @Select("select user_id,rate,income_amount from tb_trade_competition_result where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} and status = 1 and rate between -1 and 6 order by rate desc limit #{limit}")
    List<TradeCompetitionResult> queryRateListByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("limit") Integer limit);

//    @InsertProvider(type = CompetitionSqlProvider.class, method = "saveTradeCompetitionResult")
//    int saveTradeCompetitionResult(@Param("list") List<TradeCompetitionResult> list);

    @Select("select max(batch) from tb_trade_competition_result where org_id=#{orgId} and competition_code = #{competitionCode}")
    String queryMaxBatchByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode);

    @Delete("delete from tb_trade_competition_result where org_id= #{orgId} and competition_code = #{competitionCode} and batch < #{batch}")
    int clearTradeCompetitionResult(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch);

    @Select("select *from tb_trade_competition_result where org_id = #{orgId} and competition_code  = #{competitionCode} and transfer_status = 0")
    List<TradeCompetitionResult> queryWaitingTradeCompetitionList(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode);

    @Update("update tb_trade_competition_result set transfer_status = 1 where id = #{id}")
    int updateTradeCompetitionStatus(@Param("id") Long id);

    @Select("select * from tb_trade_competition_result where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} and user_id = #{userId} ")
    TradeCompetitionResult queryByUserIdAndBatch(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("userId") Long userId, @Param("batch") String batch);

    @Select("select user_id,rate,income_amount from tb_trade_competition_result where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} and status = 1 order by income_amount desc limit #{limit}")
    List<TradeCompetitionResult> listIncomeAmountByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("limit") Integer limit);

    @Select("select user_id,rate,income_amount from tb_trade_competition_result where org_id=#{orgId} and competition_code = #{competitionCode} and batch = #{batch} and user_id = #{userId}")
    TradeCompetitionResult queryUserPerformance(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("batch") String batch, @Param("userId")Long userId);
}

