package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.SpecialInterest;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-02-25 18:50
 */
@Mapper
@Component
public interface SpecialInterestMapper extends tk.mybatis.mapper.common.Mapper<SpecialInterest> {

    @Select("select * from tb_margin_special_interest where org_id = #{orgId} and account_id = #{accountId} and token_id = #{tokenId} and effective_flag = 1")
    SpecialInterest getEffectiveSpecialInterest(@Param("orgId") Long orgId, @Param("accountId") Long accountId, @Param("tokenId") String tokenId);

    @Select("select * from tb_margin_special_interest where org_id = #{orgId} and account_id = #{accountId} and token_id = #{tokenId}")
    SpecialInterest getSpecialInterest(@Param("orgId") Long orgId, @Param("accountId") Long accountId, @Param("tokenId") String tokenId);

    @Delete("delete from tb_margin_special_interest where org_id = #{orgId} and account_id = #{accountId} and token_id = #{tokenId}")
    int delSpecialInterest(@Param("orgId") Long orgId, @Param("accountId") Long accountId, @Param("tokenId") String tokenId);
}
