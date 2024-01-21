package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteBonusDetail;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface InviteBonusDetailMapper extends tk.mybatis.mapper.common.Mapper<InviteBonusDetail>, InsertListMapper<InviteBonusDetail> {
}
