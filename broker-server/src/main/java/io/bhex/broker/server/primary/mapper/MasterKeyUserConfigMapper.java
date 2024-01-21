package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MasterKeyUserConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface MasterKeyUserConfigMapper extends tk.mybatis.mapper.common.Mapper<MasterKeyUserConfig> {

    @Select("select *from tb_master_key_user_config where org_id = #{orgId} and user_id = #{userId}")
    MasterKeyUserConfig selectMasterKeyUserConfigByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);
}
