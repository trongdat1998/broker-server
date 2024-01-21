package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceTransfer;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface BalanceTransferMapper extends tk.mybatis.mapper.common.Mapper<BalanceTransfer> {

    @Select(" SELECT * FROM tb_balance_transfer WHERE org_id=#{orgId} AND client_order_id=#{clientOrderId}")
    BalanceTransfer getByClientOrderId(@Param("orgId") Long orgId, @Param("clientOrderId") String clientOrderId);

}
