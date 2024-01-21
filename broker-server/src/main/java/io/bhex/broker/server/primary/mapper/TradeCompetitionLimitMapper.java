package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.TradeCompetitionLimit;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;


@Mapper
public interface TradeCompetitionLimitMapper extends tk.mybatis.mapper.common.Mapper<TradeCompetitionLimit> {

    @Select("select *from tb_trade_competition_limit where org_id=#{orgId} and competition_code = #{competitionCode}")
    TradeCompetitionLimit queryByCode(@Param("orgId") Long orgId, @Param("competitionCode") String competitionCode);

    @Select("select *from tb_trade_competition_limit where id=#{id} for update")
    TradeCompetitionLimit lock(@Param("id") Long id);

    @Update("update tb_trade_competition_limit set enter_id_str = #{enterIdStr} where id=#{id}")
    int updateEnterUser(@Param("enterIdStr") String enterIdStr, @Param("id") Long id);
}

