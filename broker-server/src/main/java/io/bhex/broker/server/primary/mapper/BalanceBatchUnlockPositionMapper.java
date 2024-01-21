package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceBatchUnlockPosition;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface BalanceBatchUnlockPositionMapper extends tk.mybatis.mapper.common.Mapper<BalanceBatchUnlockPosition>, InsertListMapper<BalanceBatchUnlockPosition> {
}
