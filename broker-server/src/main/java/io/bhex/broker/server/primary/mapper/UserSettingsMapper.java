/*
 ************************************
 * @项目名称: broker
 * @文件名称: UserMapper
 * @Date 2018/05/22
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserSettings;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;


@Mapper
public interface UserSettingsMapper extends tk.mybatis.mapper.common.Mapper<UserSettings> {

    @Update("update tb_user_settings set common_config = #{commonConfig}, updated = #{updated} where org_id = #{orgId} and user_id = #{userId}")
    int updateCommonConfig(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("commonConfig")String commonConfig, @Param("updated")Long updated);

    @Select("select * from tb_user_settings where org_id = #{orgId} and user_id = #{userId}")
    UserSettings getByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);
}


