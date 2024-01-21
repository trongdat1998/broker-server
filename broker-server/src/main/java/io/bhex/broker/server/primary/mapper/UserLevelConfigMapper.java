package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.UserLevelConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Mapper
public interface UserLevelConfigMapper extends tk.mybatis.mapper.common.Mapper<UserLevelConfig> {

    @Select("select * from tb_user_level_config where org_id = #{orgId} and status > 0")
    List<UserLevelConfig> listOrgConfigs(@Param("orgId") long orgId);

    @Select("select * from tb_user_level_config where status = 1 order by level_position asc")
    List<UserLevelConfig> listAvailableConfigs();

    @Select("select * from tb_user_level_config where org_id = #{orgId} and id = #{levelConfigId}")
    UserLevelConfig getConfig(@Param("orgId") long orgId, @Param("levelConfigId") long levelConfigId);

    @Update("update tb_user_level_config set status = -1,updated = #{updated} where org_id = #{orgId} and id = #{id} and is_base_level = 0")
    int deleteConfig(@Param("orgId") long orgId, @Param("id") long id, @Param("updated") long updated);
}
