/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AppPushDevice;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface AppPushDeviceMapper{

    @InsertProvider(type = AppPushDeviceSqlProvider.class, method = "insert")
    int insert(AppPushDevice appPushDevice);

    @UpdateProvider(type = AppPushDeviceSqlProvider.class, method = "update")
    int update(AppPushDevice appPushDevice);

    @DeleteProvider(type = AppPushDeviceSqlProvider.class, method = "delete")
    int delete(Long id);

    @SelectProvider(type = AppPushDeviceSqlProvider.class, method = "query")
    int query(Long id);

    @SelectProvider(type = AppPushDeviceSqlProvider.class, method = "queryByUserId")
    List<AppPushDevice> queryByUserId(AppPushDevice appPushDevice);

    @SelectProvider(type = AppPushDeviceSqlProvider.class, method = "queryByUserIds")
    List<AppPushDevice> queryByUserIds(Long orgId, List<Long> userIds);

    @Select("select id, app_id, device_type, app_channel, device_token, third_push_type, push_limit, language from tb_app_push_device where org_id = #{orgId} and id > #{startId} order by id asc limit #{limit}")
    List<AppPushDevice> queryByOrgId(@Param("orgId") Long orgId, @Param("startId") Long startId, @Param("limit") Integer limit);

    @SelectProvider(type = AppPushDeviceSqlProvider.class, method = "queryByDeviceToken")
    List<AppPushDevice> queryByDeviceToken(AppPushDevice appPushDevice);

    @SelectProvider(type = AppPushDeviceSqlProvider.class, method = "queryByUserIdForAddDevice")
    List<AppPushDevice> queryByUserIdForAddDevice(AppPushDevice appPushDevice);

    @Update("update tb_app_push_device set push_limit = 2 where app_id = #{appId} and device_token = #{deviceToken}")
    int cancelUserTokenPush(@Param("appId") String appId, @Param("deviceToken") String deviceToken);

}
