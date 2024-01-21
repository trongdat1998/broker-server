package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.TradeCompetitionParticipant;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;


@Mapper
public interface TradeCompetitionParticipantMapper extends tk.mybatis.mapper.common.Mapper<TradeCompetitionParticipant>,
        InsertListMapper<TradeCompetitionParticipant> {

}

