package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingAsset;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface StakingAssetBatchMapper extends InsertListMapper<StakingAsset> {

}
