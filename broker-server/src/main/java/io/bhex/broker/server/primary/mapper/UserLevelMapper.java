package io.bhex.broker.server.primary.mapper;

import com.google.api.client.util.Lists;
import io.bhex.broker.server.model.UserLevel;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
@Mapper
public interface UserLevelMapper extends tk.mybatis.mapper.common.Mapper<UserLevel>, InsertListMapper<UserLevel> {

    @Update("update tb_user_level set status = -1,updated=#{updated} where org_id = #{brokerId} and level_config_id = #{levelConfigId}")
    int deleteUserLevel(@Param("brokerId") long brokerId, @Param("levelConfigId") long levelConfigId, @Param("updated") long updated);



    @Select("select * from tb_user_level where org_id = #{brokerId} and user_id = #{userId} and status in (1,3)")
    List<UserLevel> queryMyAvailableConfigs(@Param("brokerId") long brokerId, @Param("userId") long userId);

    @Select("select ifnull(count(*), 0) from tb_user_level where org_id = #{brokerId} and level_config_id = #{levelConfigId} and status = #{status}")
    int countLevelUsers(@Param("brokerId") long brokerId, @Param("levelConfigId") long levelConfigId, @Param("status") int status);

    @Select("update tb_user_level set status = #{status} where org_id = #{brokerId} and level_config_id = #{levelConfigId} and status > 0")
    Integer updateLevelUserStatus(@Param("brokerId") long brokerId, @Param("levelConfigId") long levelConfigId, @Param("status") int status);

    @Select("update tb_user_level set status = 0 where org_id = #{brokerId} and level_config_id = #{levelConfigId} and status = 3")
    Integer deleteWhiteListUsers(@Param("brokerId") long brokerId, @Param("levelConfigId") long levelConfigId);

}
