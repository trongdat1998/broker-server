package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerBasicConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 5:23 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Mapper
@Component
public interface BrokerBasicConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerBasicConfig> {

    String TABLE_NAME = " tb_broker_basic_config ";

    @Update("update " + TABLE_NAME + " set status=#{status} where broker_id=#{brokerId} and status=#{oldStatus}")
    int deleteConfig(@Param("status") Integer status, @Param("oldStatus") Integer oldStatus, @Param("brokerId") Long brokerId);
}
