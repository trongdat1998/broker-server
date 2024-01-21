package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcCurrency;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcCurrencyMapper extends Mapper<OtcCurrency> {

    @Select("select *from tb_otc_currency where code=#{code} and language=#{language}")
    OtcCurrency queryOtcCurrency(@Param("code") String code, @Param("language") String language, @Param("orgId") Long orgId);
}