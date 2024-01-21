package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

import io.bhex.broker.server.model.FrozenUserRecord;
import tk.mybatis.mapper.common.special.InsertListMapper;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/11/30
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface FrozenUserRecordMapper extends tk.mybatis.mapper.common.Mapper<FrozenUserRecord>, InsertListMapper<FrozenUserRecord> {

    String COLUMNS = "id, org_id, user_id, frozen_type, frozen_reason, start_time, end_time, status, created, updated";

    @Insert("INSERT INTO tb_frozen_user(org_id, user_id, frozen_type, frozen_reason, start_time, end_time, status, created, updated) "
            + "VALUES(#{orgId}, #{userId}, #{frozenType}, #{frozenReason}, #{startTime}, #{endTime}, #{status}, #{created}, #{updated})")
    @Options(useGeneratedKeys = true, keyColumn = "id")
    int insertRecord(FrozenUserRecord frozenUserRecord);

    @Select("SELECT " + COLUMNS + " FROM tb_frozen_user WHERE org_id=#{orgId} AND user_id=#{userId} AND frozen_type in (#{frozenType},0) AND status = 1 ORDER BY id DESC LIMIT 1")
    FrozenUserRecord getByUserIdAndFrozenType(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("frozenType") Integer frozenType);

    @Select("SELECT " + COLUMNS + " FROM tb_frozen_user WHERE org_id=#{orgId} AND user_id=#{userId} AND status=1 and frozen_type != 0 " +
            " and (#{now} > start_time and #{now} < end_time) ORDER BY id DESC")
    List<FrozenUserRecord> listByUser(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("now") Long now);

    @Update("update tb_frozen_user set status = 0 where id = #{id}")
    int unfreezeUsers(@Param("id") Long id);

//    @Select("select count(1) from tb_frozen_user t where t.user_id = #{userId} and org_id = #{orgId} and t.frozen_type = 1 and t.status = 1 and (#{time} > start_time and #{time} < end_time)")
//    int queryLoginFrozenUserCount(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("time") Long time);
}
