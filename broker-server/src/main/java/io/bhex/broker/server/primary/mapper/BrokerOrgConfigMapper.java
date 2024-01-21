package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerOrgConfig;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

@Component
@Mapper
public interface BrokerOrgConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerOrgConfig> {
    @Select("SELECT * FROM tb_broker_org_config WHERE org_id = #{orgId} limit 1")
    BrokerOrgConfig getBrokerOrgConfig(@Param("orgId") Long orgId);

    @Select("SELECT * FROM tb_broker_org_config limit 50")
    List<BrokerOrgConfig> getAllBrokerOrgConfigs();
}
