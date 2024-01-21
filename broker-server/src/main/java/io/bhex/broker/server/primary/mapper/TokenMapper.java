package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.QuoteToken;
import io.bhex.broker.server.model.Token;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 20/08/2018 9:18 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface TokenMapper extends tk.mybatis.mapper.common.Mapper<Token> {

    String TABLE_NAME = " tb_token ";

    String COLUMNS = "id, org_id, exchange_id, token_id, token_name, token_full_name, token_icon, "
            + "max_withdraw_quota, min_withdraw_quantity, max_withdraw_quantity, need_kyc_quantity, "
            + "fee_token_id, fee_token_name, fee, allow_deposit, allow_withdraw, is_high_risk_token," +
            " status, custom_order, category,extra_tag,extra_config,created";

    String QUOTE_TOKEN_COLUMNS = "id, org_id, token_id, token_name, token_icon, custom_order, status, category";

    @SelectProvider(type = TokenSqlProvider.class, method = "count")
    int countByBrokerId(@Param("brokerId") Long brokerId, @Param("tokenId") String tokenId, @Param("tokenName") String tokenName, @Param("category") Integer category);

    @SelectProvider(type = TokenSqlProvider.class, method = "queryToken")
    List<Token> queryToken(@Param("fromindex") Integer fromindex, @Param("endindex") Integer endindex, @Param("brokerId") Long brokerId, @Param("tokenId") String tokenId, @Param("tokenName") String tokenName, @Param("category") Integer category);

    @Update("update " + TABLE_NAME + " set allow_deposit=#{allowDeposit}, updated=unix_timestamp() * 1000 where token_id=#{tokenId} and org_id=#{brokerId}")
    int allowDeposit(@Param("tokenId") String tokenId, @Param("allowDeposit") Integer allowDeposit, @Param("brokerId") Long brokerId);

    @Update("update " + TABLE_NAME + " set allow_withdraw=#{allowWithdraw}, updated=unix_timestamp() * 1000 where token_id=#{tokenId} and org_id=#{brokerId}")
    int allowWithdraw(@Param("tokenId") String tokenId, @Param("allowWithdraw") Integer allowWithdraw, @Param("brokerId") Long brokerId);

    @Update("update " + TABLE_NAME + " set status=#{isPublish}, updated=unix_timestamp() * 1000 where token_id=#{tokenId} and org_id=#{brokerId}")
    int publish(@Param("tokenId") String tokenId, @Param("isPublish") Integer isPublish, @Param("brokerId") Long brokerId);

    @Update("update " + TABLE_NAME + " set is_high_risk_token=#{isHighRiskToken}, updated=unix_timestamp() * 1000 where org_id=#{orgId} and token_id=#{tokenId}")
    int setIsHighRiskToken(@Param("orgId") Long orgId, @Param("tokenId") String tokenId, @Param("isHighRiskToken") Integer isHighRiskToken);

    @Update("update " + TABLE_NAME + " set fee=#{fee}, updated=unix_timestamp() * 1000 where org_id=#{orgId} and token_id=#{tokenId}")
    int setFee(@Param("orgId") Long orgId, @Param("tokenId") String tokenId, @Param("fee") BigDecimal fee);

    @Update("update " + TABLE_NAME + " set token_icon=#{tokenIcon} where token_id=#{tokenId}")
    int updateTokenIcon(@Param("tokenId") String tokenId, @Param("tokenIcon") String tokenIcon);

    @SelectProvider(type = TokenSqlProvider.class, method = "queryAll")
    List<Token> queryAll(@Param("categories") List<Integer> categories);

    @SelectProvider(type = TokenSqlProvider.class, method = "queryAllByOrgId")
    List<Token> queryAllByOrgId(@Param("categories") List<Integer> categories, @Param("orgId") Long orgId);

    @Select("SELECT " + COLUMNS + " FROM tb_token WHERE org_id = #{orgId} AND token_id = #{tokenId}")
    Token getToken(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);

    @Select("SELECT " + COLUMNS + " FROM tb_token WHERE org_id = #{orgId} AND token_name = #{tokenName} limit 1")
    Token getTokenByTokenName(@Param("orgId") Long orgId, @Param("tokenName") String tokenName);

    @SelectProvider(type = TokenSqlProvider.class, method = "queryQuoteTokens")
    List<QuoteToken> queryQuoteTokens(@Param("categories") List<Integer> categories);

    @SelectProvider(type = TokenSqlProvider.class, method = "queryQuoteTokensByOrgId")
    List<QuoteToken> queryQuoteTokensByOrgId(@Param("categories") List<Integer> categories, @Param("orgId") Long orgId);

    @Select("SELECT id FROM tb_token WHERE token_id = #{tokenId} and org_id=#{brokerId}")
    Long getIdByTokenId(@Param("tokenId") String tokenId, @Param("brokerId") Long brokerId);

    @Select("SELECT token_id,token_name,token_full_name FROM tb_token WHERE org_id = #{orgId} AND category = #{category}")
    List<Token> querySimpleTokens(@Param("orgId") Long orgId, @Param("category") Integer category);

}
