package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.domain.staking.StakingRebateAvailableCount;
import io.bhex.broker.server.model.staking.StakingProductRebate;
import org.apache.ibatis.annotations.*;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.math.BigDecimal;
import java.util.List;

/**
 * 理财产品派息配置Batch mapper
 *
 * @author songxd
 * @date 2020-10-21
 */
@Mapper
public interface StakingProductRebateBatchMapper extends InsertListMapper<StakingProductRebate> {
}
