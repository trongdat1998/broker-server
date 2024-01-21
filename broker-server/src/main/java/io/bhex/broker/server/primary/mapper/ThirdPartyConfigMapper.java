package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ThirdPartyConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


@Mapper
public interface ThirdPartyConfigMapper extends tk.mybatis.mapper.common.Mapper<ThirdPartyConfig> {

    @Select("select *from tb_third_party_config where org_id = #{orgId} and user_id = #{userId}")
    ThirdPartyConfig queryConfigByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select("select *from tb_third_party_config where id = #{id} FOR UPDATE")
    ThirdPartyConfig queryConfigLock(@Param("id") Long id);
}
