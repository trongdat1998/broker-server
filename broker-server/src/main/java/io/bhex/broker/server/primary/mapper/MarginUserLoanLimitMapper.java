package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MarginUserLoanLimit;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface MarginUserLoanLimitMapper extends tk.mybatis.mapper.common.Mapper<MarginUserLoanLimit> {

    @Select("select * from tb_margin_user_loan_limit where org_id = #{orgId} and user_id = #{userId} and account_id = #{accountId} and token_id = #{tokenId}")
    MarginUserLoanLimit getByAccountIdAndTokenId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountId") Long accountId, @Param("tokenId") String tokenId);

    @Select("select * from tb_margin_user_loan_limit where org_id = #{orgId} and user_id = #{userId} and account_id = #{accountId} and token_id = #{tokenId} and status = 1; ")
    MarginUserLoanLimit getValidByAccountIdAndTokenId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountId") Long accountId, @Param("tokenId") String tokenId);
}
