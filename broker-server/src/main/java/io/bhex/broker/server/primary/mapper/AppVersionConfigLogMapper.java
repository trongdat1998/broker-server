package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AppVersionConfigLog;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;


@Mapper
public interface AppVersionConfigLogMapper extends tk.mybatis.mapper.common.Mapper<AppVersionConfigLog> {

    @Select("select * from tb_app_version_config_log where org_id = #{orgId} and device_type = #{deviceType} order by id desc limit #{limit}")
    List<AppVersionConfigLog> queryLogs(@Param("orgId") long orgId, @Param("deviceType") String deviceType, @Param("limit") int limit);

    @Select("select * from tb_app_version_config_log where org_id = #{orgId} and device_type = #{deviceType}" +
            " and app_channel = #{appChannel} order by id desc limit #{limit}")
    List<AppVersionConfigLog> queryLogsByChannel(@Param("orgId") long orgId, @Param("deviceType") String deviceType,
                                                 @Param("appChannel") String appChannel, @Param("limit") int limit);
}
