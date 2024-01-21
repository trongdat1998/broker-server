package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerAccountTradeFeeAdjust;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@org.apache.ibatis.annotations.Mapper
public interface BrokerAccountTradeFeeAdjustMapper extends Mapper<BrokerAccountTradeFeeAdjust> {

    @Select("select *from tb_broker_account_trade_fee_adjust where maker_fee_rate_adjust  > 0 and taker_fee_rate_adjust > 0")
    List<BrokerAccountTradeFeeAdjust> selectBrokerAccountTradeFeeAdjustAll();
}
