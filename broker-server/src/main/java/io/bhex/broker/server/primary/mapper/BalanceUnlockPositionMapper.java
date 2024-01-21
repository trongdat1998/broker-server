package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceUnlockPosition;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface BalanceUnlockPositionMapper extends tk.mybatis.mapper.common.Mapper<BalanceUnlockPosition>, InsertListMapper<BalanceUnlockPosition> {

    @Select(" SELECT * FROM tb_balance_unlock_position WHERE org_id=#{orgId} AND client_order_id=#{clientOrderId}")
    BalanceUnlockPosition getByClientOrderId(@Param("orgId") Long orgId, @Param("clientOrderId") String clientOrderId);
}