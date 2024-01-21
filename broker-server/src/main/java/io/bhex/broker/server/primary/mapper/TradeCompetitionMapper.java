package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.TradeCompetition;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;


@Mapper
public interface TradeCompetitionMapper extends tk.mybatis.mapper.common.Mapper<TradeCompetition> {

    @Select("select *from tb_trade_competition where status = 1")
    List<TradeCompetition> queryAll();

    @Select("select *from tb_trade_competition where org_id=#{orgId} and competition_code = #{competitionCode}")
    TradeCompetition queryByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode);

    @Update("update tb_trade_competition set status = 2 where org_id=#{orgId} and competition_code = #{competitionCode}")
    void updateEndStatusByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode);
}

