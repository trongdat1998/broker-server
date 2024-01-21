package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.TradeCompetitionExt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.base.insert.InsertMapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;


@Mapper
public interface TradeCompetitionExtMapper extends tk.mybatis.mapper.common.Mapper<TradeCompetitionExt>,
        tk.mybatis.mapper.common.IdsMapper<TradeCompetitionExt>, InsertListMapper<TradeCompetitionExt> {

    @Select("select *from tb_trade_competition_ext where org_id= #{orgId} and competition_code = #{competitionCode}")
    List<TradeCompetitionExt> queryTradeCompetitionExtByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode);

    @Select("select *from tb_trade_competition_ext where org_id= #{orgId} and competition_code = #{competitionCode} and language = #{language}")
    TradeCompetitionExt queryTradeCompetitionExtByLanguage(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode, @Param("language") String language);
}

