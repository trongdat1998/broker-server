package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AppIndexModuleConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface AppIndexModuleConfigMapper extends tk.mybatis.mapper.common.Mapper<AppIndexModuleConfig> {

    @Select("select * from tb_app_index_module_config where status = 1")
    List<AppIndexModuleConfig> getAllAvailableConfigs();

    @Select("select * from tb_app_index_module_config where org_id = #{orgId} and status = 1")
    List<AppIndexModuleConfig> getOrgAvailableConfigs(@Param("orgId") Long orgId);

    @Update("update tb_app_index_module_config set status = 0, custom_order = 0 where org_id = #{orgId} and module_type = #{moduleType}")
    int resetModuleConfig(@Param("orgId") Long orgId, @Param("moduleType") Integer moduleType);

    @Select("select * from tb_app_index_module_config where org_id = #{orgId} and module_type = #{moduleType} and status = 1")
    List<AppIndexModuleConfig> getModuleConfigs(@Param("orgId") Long orgId, @Param("moduleType") Integer moduleType);
}
