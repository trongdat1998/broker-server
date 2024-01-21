package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingProductRebateDetail;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

/**
 * 理财产品派息记录批量Insert
 * @author songxd
 * @date 2020-08-03
 */
@Mapper
public interface StakingProductRebateDetailBatchMapper extends InsertListMapper<StakingProductRebateDetail> {
}
