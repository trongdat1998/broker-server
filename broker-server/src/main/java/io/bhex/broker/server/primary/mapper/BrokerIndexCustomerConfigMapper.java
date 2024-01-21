package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerIndexCustomerConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Description:
 * @Date: 2020/2/11 上午7:31
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Mapper
@Component
public interface BrokerIndexCustomerConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerIndexCustomerConfig> {

    String TABLE_NAME = " tb_broker_index_customer_config ";

    @Update("delete from " + TABLE_NAME + " where broker_id=#{brokerId} and module_name = #{moduleName} and status=#{status}")
    int deleteConfigs(@Param("brokerId") Long brokerId,
                      @Param("moduleName") String moduleName,
                      @Param("status") Integer status);

    @Select("select * from " + TABLE_NAME + " where broker_id=#{brokerId} and status=#{status} "
            + " and module_name = #{moduleName} and locale = #{locale} limit 1")
    BrokerIndexCustomerConfig getByModuleName(@Param("brokerId") Long brokerId,
                                              @Param("moduleName") String moduleName,
                                              @Param("status") Integer status,
                                              @Param("locale") String locale);

    @Select("select * from " + TABLE_NAME + " where broker_id=#{brokerId} and config_type = #{configType} and status=#{status} ")
    List<BrokerIndexCustomerConfig> getBrokerConfigs(@Param("brokerId") Long brokerId, @Param("status") Integer status, @Param("configType") Integer configType);

}
