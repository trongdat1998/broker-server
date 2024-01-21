package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.UserActionLog;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserActionLogMapper extends tk.mybatis.mapper.common.Mapper<UserActionLog> {
}
