package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.TokenConvertRate;
import org.apache.ibatis.annotations.*;

import java.math.BigDecimal;
import java.util.List;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/8/29
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface TokenConvertRateMapper {

    String COLUMNS = "id, org_id, token_id, convert_token_id, rate_up_ratio, convert_rate, last_convert_rate";

    @Select("SELECT " + COLUMNS + " FROM tb_token_convert_rate WHERE org_id = #{orgId} AND token_id = #{tokenId} AND convert_token_id = #{convertTokenId}")
    TokenConvertRate getByToken(@Param("orgId") Long orgId, @Param("tokenId") String tokenId, @Param("convertTokenId") String convertTokenId);

    @Select("SELECT " + COLUMNS + " FROM tb_token_convert_rate")
    List<TokenConvertRate> queryAll();

    @Update("UPDATE tb_token_convert_rate SET convert_rate=#{convertRate}, last_convert_rate=#{lastConvertRate}, updated=#{updated} WHERE id=#{id}")
    int update(@Param("id") Long id, @Param("convertRate") BigDecimal convertRate,
               @Param("lastConvertRate") BigDecimal lastConvertRate, @Param("updated") Long updated);

    @Update("UPDATE tb_token_convert_rate SET rate_up_ratio=#{rateUpRatio} WHERE id=#{id}")
    int updateRateUpRatio(@Param("id") Long id, @Param("rateUpRatio") BigDecimal rateUpRatio);

    @Insert("INSERT INTO tb_token_convert_rate(org_id, token_id, convert_token_id, rate_up_ratio, convert_rate, last_convert_rate, created, updated) values"
            + "(#{orgId}, #{tokenId}, #{convertTokenId}, #{rateUpRatio}, #{convertRate}, #{lastConvertRate}, #{created}, #{updated})")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insert(TokenConvertRate tokenConvertRate);

}
