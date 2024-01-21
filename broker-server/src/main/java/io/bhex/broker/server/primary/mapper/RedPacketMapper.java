package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.RedPacket;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;

@Mapper
public interface RedPacketMapper extends tk.mybatis.mapper.common.Mapper<RedPacket> {

    @Select("SELECT * FROM tb_red_packet WHERE id = #{id} FOR UPDATE")
    RedPacket lockedWithId(@Param(value = "id") Long id);

    @Update("UPDATE tb_red_packet set remain_count = remain_count - 1, remain_amount = remain_amount - #{receivedAmount}, status = #{status} WHERE id = #{id}")
    int reduceRemainValue(@Param(value = "id") Long id, @Param(value = "receivedAmount") BigDecimal receivedAmount, @Param(value = "status") Integer status);

}
