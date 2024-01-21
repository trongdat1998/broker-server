package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteLevel;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface InviteLevelMapper extends tk.mybatis.mapper.common.Mapper<InviteLevel>, InsertListMapper<InviteLevel> {
}
