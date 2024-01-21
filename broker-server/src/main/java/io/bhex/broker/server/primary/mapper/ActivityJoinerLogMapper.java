package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.ActivityJoinerLog;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@org.apache.ibatis.annotations.Mapper
public interface ActivityJoinerLogMapper extends Mapper<ActivityJoinerLog> {

    @Select("select id from tb_activity_joiner_log where groupId=#{guildId} order by id desc")
    List<Long> selectFollowIds(@Param("guildId")long guildId);

    @Select("select user_id from tb_activity_joiner_log where user_id<>follower_id group by user_id having count(1)>=#{followerNumber}")
    List<Long> groupByUserId(@Param("followerNumber")int followerNumber);

    @Select("select follower_id from tb_activity_joiner_log where user_id= #{userId} and follower_id<>#{userId} ")
    List<Long> listFollowerIdByUserId(@Param("userId") Long userId);

    @Select("select group_id from tb_activity_joiner_log group by group_id having count(1)>=#{followerNumber}")
    List<Long> distinctGroupId(@Param("followerNumber")int followerNumber);

    @Select("select follower_id from tb_activity_joiner_log where group_id= #{groupId}")
    List<Long> listFollowerIdByGroupId(@Param("groupId")Long groupId);
}
