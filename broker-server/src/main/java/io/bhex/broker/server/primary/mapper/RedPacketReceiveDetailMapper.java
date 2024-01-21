package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.RedPacketReceiveDetail;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface RedPacketReceiveDetailMapper extends tk.mybatis.mapper.common.Mapper<RedPacketReceiveDetail>, InsertListMapper<RedPacketReceiveDetail> {

    @Select("SELECT * FROM tb_red_packet_receive_detail WHERE org_id=#{orgId} AND red_packet_id=#{redPacketId} AND assign_index=#{assignIndex} FOR UPDATE")
    RedPacketReceiveDetail lockWithIndex(@Param(value = "orgId") Long orgId,
                                         @Param(value = "redPacketId") Long redPacketId,
                                         @Param(value = "assignIndex") Integer assignIndex);


    @Select("SELECT * FROM tb_red_packet_receive_detail WHERE id=#{id} FOR UPDATE")
    RedPacketReceiveDetail lockWithId(@Param(value = "id") Long id);
}
