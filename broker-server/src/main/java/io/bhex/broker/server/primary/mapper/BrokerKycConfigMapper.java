package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerKycConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface BrokerKycConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerKycConfig> {

    @Select("select * from tb_broker_kyc_config where org_id = #{brokerId} and status = 1")
    List<BrokerKycConfig> queryByBrokerId(@Param("brokerId") long brokerId);
}
