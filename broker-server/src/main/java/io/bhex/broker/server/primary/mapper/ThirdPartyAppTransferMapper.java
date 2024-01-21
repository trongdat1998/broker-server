package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ThirdPartyAppTransfer;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface ThirdPartyAppTransferMapper extends tk.mybatis.mapper.common.Mapper<ThirdPartyAppTransfer> {

    @Select("SELECT * FROM tb_third_party_app_transfer WHERE org_id=#{orgId} AND third_party_app_id=#{thirdPartyAppId} AND client_order_id=#{clientOrderId}")
    ThirdPartyAppTransfer getByAppIdAndClientOrderId(@Param("orgId") Long orgId, @Param("thirdPartyAppId") Long thirdPartyAppId, @Param("clientOrderId") String clientOrderId);

}
