/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/12/21
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AppVersionInfo;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface AppVersionInfoMapper extends tk.mybatis.mapper.common.Mapper<AppVersionInfo> {

    @Select("select * from tb_app_version_info")
    List<AppVersionInfo> queryAll();

    @Select("select * from tb_app_version_info where org_id = #{orgId}")
    List<AppVersionInfo> queryOrgAppVersionInfo(@Param(value = "orgId") Long orgId);

    @Select("select * from tb_app_version_info where org_id = #{orgId} and device_type = #{deviceType} order by id desc limit 1")
    AppVersionInfo queryLastAppVersion(@Param("orgId") long orgId, @Param("deviceType") String deviceType);

    @Select("select * from tb_app_version_info where org_id = #{orgId} and app_id=#{appId} and app_version=#{appVersion} and device_type = #{deviceType}")
    AppVersionInfo queryOneAppVersion(@Param("orgId") long orgId, @Param("appId") String appId,
                                      @Param("appVersion") String appVersion, @Param("deviceType") String deviceType);

    @Update("update tb_app_version_info set is_latest_version = #{isLatestVersion} where org_id = #{orgId} and device_type = #{deviceType}")
    int updateLatestVerison(@Param("orgId") long orgId, @Param("deviceType") String deviceType, @Param("isLatestVersion") int isLatestVersion);

    @Select("select * from tb_app_version_info where is_latest_version = 1")
    List<AppVersionInfo> queryAllLatest();

    @Select("select * from tb_app_version_info where org_id = #{orgId} and is_latest_version = 1")
    List<AppVersionInfo> queryOrgLatest(@Param(value = "orgId") Long orgId);

    @Select("select * from tb_app_version_info where org_id = #{orgId} and device_type=#{deviceType}"
            + " order by id asc")
    List<AppVersionInfo> queryAllVersions(@Param("orgId") long orgId, @Param("deviceType") String deviceType);


    @Select("select * from tb_app_version_info where org_id = #{orgId} and app_id=#{appId} and device_type = #{deviceType}")
    List<AppVersionInfo> queryAppVersionByDeviceType(@Param("orgId") long orgId, @Param("appId") String appId, @Param("deviceType") String deviceType);

}
