/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AppVersionConfig;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface AppVersionConfigMapper extends tk.mybatis.mapper.common.Mapper<AppVersionConfig> {

    @SelectProvider(type = AppVersionConfigSqlProvider.class, method = "getByAppVersion")
    AppVersionConfig getByAppVersion(AppVersionConfig versionConfig);

    @SelectProvider(type = AppVersionConfigSqlProvider.class, method = "queryAll")
    List<AppVersionConfig> queryAll();

    @Select("select distinct(app_channel) from tb_app_version_config where org_id = #{orgId} and device_type=#{deviceType}")
    List<String> getAppChannels(@Param("orgId") Long orgId, @Param("deviceType") String deviceType);

    @Select("select * from tb_app_version_config where enable_status = 1")
    List<AppVersionConfig> queryAllEnableConfigs();

    @Select("select * from tb_app_version_config where org_id=#{orgId} and enable_status = 1")
    List<AppVersionConfig> queryOrgEnableConfigs(@Param(value = "orgId") Long orgId);

    @Update("UPDATE tb_app_version_config SET need_update=1, update_version=#{updateVersion} WHERE org_id=#{orgId} and device_type=#{deviceType}")
    int setAppUpdate(@Param("orgId") Long orgId, @Param("deviceType") String deviceType, @Param("updateVersion") String updateVersion);


    @Update("UPDATE tb_app_version_config SET "
            + " need_update = IF(app_version  <> #{updateVersion}, 1, 0),"
            + "need_force_update=0, update_version=#{updateVersion} "
            + "WHERE org_id=#{orgId} and device_type=#{deviceType} and app_version between #{minVersion} and #{maxVersion}")
    int setAppNeedUpdate(@Param("orgId") Long orgId, @Param("deviceType") String deviceType,
                         @Param("minVersion") String minVersion, @Param("maxVersion") String maxVersion,
                         @Param("updateVersion") String updateVersion);


    @Update("UPDATE tb_app_version_config SET "
            + " need_force_update = IF(app_version  <> #{updateVersion}, 1, 0),"
            + "need_update=0, update_version=#{updateVersion} "
            + "WHERE org_id=#{orgId} and device_type=#{deviceType} and app_version between #{minVersion} and #{maxVersion}")
    int setAppNeedForceUpdate(@Param("orgId") Long orgId, @Param("deviceType") String deviceType,
                         @Param("minVersion") String minVersion, @Param("maxVersion") String maxVersion,
                         @Param("updateVersion") String updateVersion);

    @Select("select * from tb_app_version_config where org_id=#{orgId} and device_type=#{deviceType}  and app_channel=#{appChannel} "
            + "and update_version = #{updateVersion} and app_version = #{updateVersion} limit 1")
    AppVersionConfig getByUpdateVersion(@Param("orgId") Long orgId, @Param("deviceType") String deviceType, @Param("appChannel") String appChannel,
                                        @Param("updateVersion") String updateVersion);


    @Select("select app_version from tb_app_version_config where org_id = #{orgId} and device_type=#{deviceType} and app_channel=#{appChannel} "
            + " order by id asc")
    List<String> queryAllVersions(@Param("orgId") long orgId, @Param("deviceType") String deviceType, @Param("appChannel") String appChannel);

    @Select("select * from tb_app_version_config where org_id = #{orgId} and device_type=#{deviceType}"
            + " order by id asc")
    List<AppVersionConfig> queryMyVersionConfigs(@Param("orgId") long orgId, @Param("deviceType") String deviceType);


}
