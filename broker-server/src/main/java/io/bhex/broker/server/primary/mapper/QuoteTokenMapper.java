package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.QuoteToken;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;


/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 27/11/2018 5:20 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface QuoteTokenMapper extends Mapper<QuoteToken> {

    @Update("update tb_quote_token set custom_order=#{customOrder} where org_id=#{orgId} and token_id=#{tokenId}")
    int updateCustomOrder(@Param("orgId") Long orgId, @Param("tokenId") String tokenId, @Param("customOrder") int customOrder);

    @Update("delete from tb_quote_token where org_id=#{orgId} and category = 1")
    int deleteQuoteTokens(@Param("orgId") Long orgId);
}
