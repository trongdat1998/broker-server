package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteTokenConversion;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface InviteTokenConversionMapper extends tk.mybatis.mapper.common.Mapper<InviteTokenConversion>, InsertListMapper<InviteTokenConversion> {
}
