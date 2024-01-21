package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.RedPacketConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface RedPacketConfigMapper extends tk.mybatis.mapper.common.Mapper<RedPacketConfig> {

    @Select("SELECT * FROM tb_red_packet_config WHERE org_id = #{orgId}")
    RedPacketConfig getRedPacketConfig(@Param(value = "orgId") Long orgId);

}
