package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.RedPacketTokenConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface RedPacketTokenConfigMapper extends tk.mybatis.mapper.common.Mapper<RedPacketTokenConfig> {

    @Select("SELECT * FROM tb_red_packet_token_config WHERE org_id = #{orgId}")
    List<RedPacketTokenConfig> queryOrgRedPacketTokenConfig(@Param(value = "orgId") Long orgId);

    @Update("UPDATE tb_red_packet_token_config set custom_order = #{customOrder}, updated = #{updated}  WHERE org_id=#{orgId} AND id = #{id}")
    int updateCustomOrder(@Param(value = "orgId") Long orgId, @Param(value = "id") Long id, @Param(value = "customOrder") Integer customOrder, @Param(value = "updated") Long updated);

}
