package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcThirdPartyPayment;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface OtcThirdPartyPaymentMapper extends tk.mybatis.mapper.common.Mapper<OtcThirdPartyPayment> {
    @Select("SELECT * FROM tb_otc_third_party_payment WHERE third_party_id=#{thirdPartyId}" +
            " AND payment_id=#{paymentId} AND token_id=#{tokenId} AND currency_id=#{currencyId};")
    OtcThirdPartyPayment getByPaymentId(@Param("thirdPartyId") Long thirdPartyId,
                                        @Param("paymentId") Long paymentId,
                                        @Param("tokenId") String tokenId,
                                        @Param("currencyId") String currencyId);

}
