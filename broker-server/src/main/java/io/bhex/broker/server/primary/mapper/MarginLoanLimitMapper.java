package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MarginLoanLimit;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface MarginLoanLimitMapper extends tk.mybatis.mapper.common.Mapper<MarginLoanLimit> {

    @Select("select * from tb_margin_loan_limit where org_id = #{orgId} and token_id = #{tokenId};")
    MarginLoanLimit getByOrgIdAndTokenId(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);

    @Select("select * from tb_margin_loan_limit where org_id = #{orgId} and token_id = #{tokenId} and status = 1;")
    MarginLoanLimit getValidByOrgIdAndTokenId(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);

}
