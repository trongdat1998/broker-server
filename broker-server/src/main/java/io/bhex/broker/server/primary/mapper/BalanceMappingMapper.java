package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceMapping;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface BalanceMappingMapper extends tk.mybatis.mapper.common.Mapper<BalanceMapping> {

    @Select("SELECT * FROM tb_balance_mapping WHERE org_id=#{orgId} AND client_order_id=#{clientOrderId} FOR UPDATE")
    BalanceMapping getByClientOrderId(@Param("orgId") Long orgId, @Param("clientOrderId") String clientOrderId);

}
