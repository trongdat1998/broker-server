package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingAssetSnapshot;
import io.bhex.broker.server.model.staking.StakingPoolRebateDetail;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.math.BigDecimal;
import java.util.List;

/**
 * 资产快照
 *
 * @author generator
 * @date 2020-08-21
 */
@Mapper
public interface StakingAssetSnapshotBatchMapper extends InsertListMapper<StakingAssetSnapshot> {
}
