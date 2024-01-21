package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteUserFee;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface InviteUserFeeMapper extends tk.mybatis.mapper.common.Mapper<InviteUserFee>, InsertListMapper<InviteUserFee> {
}
