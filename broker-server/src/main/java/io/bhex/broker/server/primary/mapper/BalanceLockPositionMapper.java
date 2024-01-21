package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceLockPosition;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface BalanceLockPositionMapper extends tk.mybatis.mapper.common.Mapper<BalanceLockPosition>, InsertListMapper<BalanceLockPosition> {

    @Select(" SELECT * FROM tb_balance_lock_position WHERE org_id=#{orgId} AND client_order_id=#{clientOrderId}")
    BalanceLockPosition getByClientOrderId(@Param("orgId") Long orgId, @Param("clientOrderId") String clientOrderId);
}