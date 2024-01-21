package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcLegalCurrency;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;


@org.apache.ibatis.annotations.Mapper
public interface OtcLegalCurrencyMapper extends Mapper<OtcLegalCurrency> {

    @Select("select * from tb_otc_legal_currency where org_id = #{orgId} and user_id = #{userId}")
    OtcLegalCurrency getLegalCurrencyByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Update("update tb_otc_legal_currency set code = #{code} where org_id = #{orgId} and user_id = #{userId}")
    int updateCodeByOrgIdAndUserId(@Param("code") String code, @Param("orgId") Long orgId, @Param("userId") Long userId);
}
