package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.Country;
import io.bhex.broker.server.model.CountryDetail;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/7/6
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface CountryMapper {

    String COLUMNS = "id, national_code, domain_short_name, custom_order, currency_code, country_name, for_area_code, for_nationality";
    String DETAIL_COLUMNS = "id, country_id, country_name, index_name, language";

    @Select("SELECT " + COLUMNS + " FROM tb_country WHERE `status`=1")
    List<Country> queryAllCountry();

    @Select("SELECT " + DETAIL_COLUMNS + " FROM tb_country_detail")
    List<CountryDetail> queryAllCountryDetail();

    @Select("SELECT " + COLUMNS + " FROM tb_country where id = #{id}")
    Country queryCountryById(@Param("id") Long id);

    @Select("SELECT " + COLUMNS + " FROM tb_country where national_code = #{code}")
    Country queryCountryByCode(@Param("code") String code);
}
