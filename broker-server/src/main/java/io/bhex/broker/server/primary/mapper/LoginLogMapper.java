package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.LoginLog;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.service.user
 *@Date 2018/8/3
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface LoginLogMapper {

    @Insert("INSERT INTO tb_login_log (org_id, user_id, ip, region, status, login_type, platform, user_agent, `language`, app_base_header, channel, source, created, updated) "
            + "VALUES(#{orgId}, #{userId}, #{ip}, #{region}, #{status}, #{loginType}, #{platform}, #{userAgent}, #{language}, #{appBaseHeader}, #{channel}, #{source}, #{created}, #{updated})")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insert(LoginLog loginLog);

    @Insert("UPDATE tb_login_log SET status = #{status}, updated=#{updated} WHERE id = #{id}")
    int update(LoginLog loginLog);

    @SelectProvider(type = LoginLogSqlProvider.class, method = "queryLog")
    List<LoginLog> queryLog(@Param("userId") Long userId, @Param("startTime") Long startTime,
                            @Param("endTime") Long endTime, @Param("fromId") Long fromId, @Param("limit") Integer limit);

    @SelectProvider(type = LoginLogSqlProvider.class, method = "getLogs")
    List<LoginLog> getLogs(@Param("userId") Long userId,
                           @Param("startTime") Long startTime, @Param("endTime") Long endTime,
                           @Param("start") Integer start, @Param("offset") Integer offset);

    @SelectProvider(type = LoginLogSqlProvider.class, method = "countLogs")
    Integer countLogs(@Param("userId") Long userId, @Param("startTime") Long startTime, @Param("endTime") Long endTime);

    @SelectProvider(type = LoginLogSqlProvider.class, method = "getLastLoginLog")
    LoginLog getLastLoginLog(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("platform") String platform);

    @Select("SELECT * FROM tb_login_log WHERE org_id=#{orgId} AND user_id=#{userId} AND platform=#{platform} AND status=1 ORDER BY id desc LIMIT 1")
    LoginLog getLastSuccessLog(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("platform") String platform);

    @SelectProvider(type = LoginLogSqlProvider.class, method = "getById")
    LoginLog getById(Long id);

    @Select("select *from tb_login_log where user_id = #{userId} order by created desc limit #{limit}")
    List<LoginLog> queryLastLoginLogs(@Param("userId") Long userId, @Param("limit") int limit);

    @Select("select distinct user_id, platform, user_agent,org_id from tb_login_log where created between #{start} and #{end}")
    List<LoginLog> queryOneDayLoginLogList(@Param("start") Long start, @Param("end") Long end);

    @Select("SELECT org_id,user_id,max(created) as created FROM tb_login_log WHERE org_id=#{orgId} AND status=1 AND created<#{inactiveTime} GROUP BY user_id")
    List<LoginLog> queryInactiveUser(@Param("orgId") Long orgId, @Param("inactiveTime") Long inactiveTime);

    @Select("SELECT * FROM tb_login_log WHERE org_id=#{orgId} AND user_id=#{userId}  AND status=1 ORDER BY id desc LIMIT 1")
    LoginLog getAllPlatfromLastSuccessLog(@Param("orgId") Long orgId, @Param("userId") Long userId);

}
