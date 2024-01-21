package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.domain.staking.UserCurrentOrderSum;
import io.bhex.broker.server.model.staking.StakingProductOrder;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.math.BigDecimal;
import java.util.List;

/**
 * 理财申购自定义Mapper
 *
 * @author songxd
 * @date 2020-07-31
 */
public interface StakingProductOrderBatchMapper extends InsertListMapper<StakingProductOrder> {

}
