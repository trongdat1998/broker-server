package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcTradeFeeRate;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

@org.apache.ibatis.annotations.Mapper
public interface OtcTradeFeeRateMapper extends Mapper<OtcTradeFeeRate> {

    @Select("select *from tb_otc_trade_fee_rate where org_id = #{orgId} and token_id= #{tokenId} limit 1")
    OtcTradeFeeRate queryOtcTradeFeeByTokenId(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);
}
