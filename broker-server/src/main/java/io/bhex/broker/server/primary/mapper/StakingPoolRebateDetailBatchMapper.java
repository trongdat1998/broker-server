package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingPoolRebateDetail;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface StakingPoolRebateDetailBatchMapper extends InsertListMapper<StakingPoolRebateDetail> {
}
