package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerDictConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

@Mapper
@Component
public interface BrokerDictConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerDictConfig> {

    @Select("SELECT * FROM tb_broker_dict_config WHERE org_id = #{orgId} AND config_key = #{configKey}")
    BrokerDictConfig getBrokerDictConfigByKey(@Param("orgId") Long orgId, @Param("configKey") String configKey);
}
