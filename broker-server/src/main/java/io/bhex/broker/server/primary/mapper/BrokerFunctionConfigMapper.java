package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerFunctionConfig;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface BrokerFunctionConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerFunctionConfig> {

    @Insert("insert into tb_broker_function_config (broker_id, function, status) values (#{orgId}, #{function}, "
        + "#{status}) on duplicate key update status = #{status}")
    int setBrokerFunctionConfig(@Param("orgId") Long orgId,
                                @Param("function") String function,
                                @Param("status") Integer status);
}
