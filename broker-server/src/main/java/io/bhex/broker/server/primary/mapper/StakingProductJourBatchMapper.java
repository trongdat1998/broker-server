package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingProductJour;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

@Mapper
public interface StakingProductJourBatchMapper extends InsertListMapper<StakingProductJour> {
}