package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

import io.bhex.broker.server.model.ActivityUserCard;
import tk.mybatis.mapper.common.Mapper;

@org.apache.ibatis.annotations.Mapper
public interface ActivityUserCardMapper extends Mapper<ActivityUserCard> {

    String TINY_COLUMNS = "id, user_id, par_value, activity_id, used_time, update_time,";

    @Select("select * from tb_activity_user_card where user_id=#{userId} and card_status=#{status} limit #{offset},#{size} ")
    List<ActivityUserCard> selectByPge(@Param("userId") long userId, @Param("offset") int offset, @Param("size") int size, @Param("status") byte status);

    @Select("select * from tb_activity_user_card where user_id=#{userId} and card_status<>0 limit #{offset},#{size} ")
    List<ActivityUserCard> selectAllByPge(@Param("userId") long userId, @Param("offset") int offset, @Param("size") int size);

    @Select("select * from tb_activity_user_card where card_status=#{status} and card_type=#{cardType} limit #{offset},#{size} ")
    List<ActivityUserCard> selectUserCard(@Param("status") int status, @Param("cardType") int cardType, @Param("offset") int offset, @Param("size") int size);

    @Select("select id from tb_activity_user_card where card_status=#{status} and card_type=#{cardType} limit #{offset},#{size} ")
    List<Long> selectUserCardIds(@Param("status") int status, @Param("cardType") int cardType, @Param("offset") int offset, @Param("size") int size);

    @Select("select * from tb_activity_user_card where card_status=#{status} and card_type=#{cardType} and id>#{id} order by id limit #{size} ")
    List<ActivityUserCard> selectUserCardByPage(@Param("status") int status, @Param("cardType") int cardType, @Param("id") long id, @Param("size") int size);
}
