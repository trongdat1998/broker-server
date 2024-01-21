package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ConvertSymbol;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface ConvertSymbolMapper extends tk.mybatis.mapper.common.Mapper<ConvertSymbol> {
    @Select("SELECT * FROM tb_convert_symbol WHERE id=#{convertSymbolId} AND broker_id=#{brokerId}")
    ConvertSymbol getByConvertSymbolId(@Param("convertSymbolId") Long convertSymbolId,
                                       @Param("brokerId") Long brokerId);

    @Select("SELECT * FROM tb_convert_symbol WHERE broker_id=#{brokerId} AND purchase_token_id=#{purchaseTokenId} AND offerings_token_id=#{offeringsTokenId}")
    ConvertSymbol getByBrokerAndToken(@Param("brokerId") Long brokerId,
                                      @Param("purchaseTokenId") String purchaseTokenId,
                                      @Param("offeringsTokenId") String offeringsTokenId);

    @Select("SELECT * FROM tb_convert_symbol WHERE broker_id=#{brokerId}")
    List<ConvertSymbol> querySymbolsByBrokerId(@Param("brokerId") Long brokerId);

    @Select("SELECT offerings_token_id FROM tb_convert_symbol WHERE broker_id=#{brokerId} AND status=#{status}")
    List<String> queryOfferingsTokens(@Param("brokerId") Long brokerId, @Param("status") Integer status);

    @Select("SELECT purchase_token_id FROM tb_convert_symbol WHERE broker_id=#{brokerId} AND status=#{status}")
    List<String> queryPurchaseTokens(@Param("brokerId") Long brokerId, @Param("status") Integer status);
}
