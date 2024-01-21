package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityWelfareCardLog;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@org.apache.ibatis.annotations.Mapper
public interface ActivityWelfareCardLogMapper extends Mapper<ActivityWelfareCardLog> {

    @Select("select * from tb_activity_welfare_log where group_id=#{guildId} limit #{offset},#{size}")
    List<ActivityWelfareCardLog> selectByPage(@Param("guildId") long guildId,
                                              @Param("offset") int offset, @Param("size") int size);
}
