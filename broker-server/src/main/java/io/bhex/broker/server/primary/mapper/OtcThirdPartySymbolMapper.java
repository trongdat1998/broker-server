package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcThirdPartySymbol;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface OtcThirdPartySymbolMapper extends tk.mybatis.mapper.common.Mapper<OtcThirdPartySymbol> {
    @Select("SELECT * FROM tb_otc_third_party_symbol WHERE org_id=#{orgId} AND third_party_id=#{thirdPartyId} AND side=#{side};")
    List<OtcThirdPartySymbol> queryListByOrgId(@Param("orgId") Long orgId,
                                               @Param("thirdPartyId") Long thirdPartyId,
                                               @Param("side") Integer side);

    @Select("SELECT * FROM tb_otc_third_party_symbol WHERE org_id=#{orgId} AND third_party_id=#{thirdPartyId} AND side=#{side}" +
            " AND token_id=#{tokenId} AND currency_id=#{currencyId};")
    List<OtcThirdPartySymbol> queryListByTokenId(@Param("orgId") Long orgId,
                                                 @Param("tokenId") String tokenId,
                                                 @Param("currencyId") String currencyId,
                                                 @Param("thirdPartyId") Long thirdPartyId,
                                                 @Param("side") Integer side);

    @Select("SELECT * FROM tb_otc_third_party_symbol WHERE org_id=#{orgId} AND third_party_id=#{thirdPartyId} AND side=#{side}" +
            " AND token_id=#{tokenId} AND currency_id=#{currencyId} AND payment_id=#{paymentId} LIMIT 1;")
    OtcThirdPartySymbol getByTokenAndPayment(@Param("orgId") Long orgId,
                                             @Param("thirdPartyId") Long thirdPartyId,
                                             @Param("tokenId") String tokenId,
                                             @Param("currencyId") String currencyId,
                                             @Param("paymentId") Long paymentId,
                                             @Param("side") Integer side);

    @Select("SELECT * FROM tb_otc_third_party_symbol WHERE org_id=#{orgId} AND otc_symbol_id=#{otcSymbolId} AND side=#{side};")
    OtcThirdPartySymbol getBySymbolId(@Param("orgId") Long orgId,
                                      @Param("otcSymbolId") Long otcSymbolId,
                                      @Param("side") Integer side);

}
