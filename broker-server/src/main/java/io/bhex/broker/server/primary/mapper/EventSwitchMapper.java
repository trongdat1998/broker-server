package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.EventSwitch;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.sql.Timestamp;

/**
 * 事件开关数据 Mapper
 *
 * @author zhaojiankun
 * @date 2018/09/14
 */
@org.apache.ibatis.annotations.Mapper
public interface EventSwitchMapper extends Mapper<EventSwitch> {

    String TABLE_NAME = "tb_event_switch";

    @Update("update " + TABLE_NAME + " set is_open = #{isOpen} , close_end_time = #{closeEndTime} , updated_at = #{now} where event_switch_id = #{eventSwitchId} ")
    int updateIsOpen(@Param("eventSwitchId") String eventSwitchId, @Param("isOpen") Boolean isOpen,
                     @Param("closeEndTime") Timestamp closeEndTime, @Param("now") Timestamp now);

}
