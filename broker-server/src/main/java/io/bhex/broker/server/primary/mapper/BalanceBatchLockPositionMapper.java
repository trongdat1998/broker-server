package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceBatchLockPosition;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface BalanceBatchLockPositionMapper extends tk.mybatis.mapper.common.Mapper<BalanceBatchLockPosition>, InsertListMapper<BalanceBatchLockPosition> {
}
