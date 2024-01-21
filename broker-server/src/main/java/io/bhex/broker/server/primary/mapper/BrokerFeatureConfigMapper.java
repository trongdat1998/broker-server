package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerFeatureConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 11:46 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Mapper
@Component
public interface BrokerFeatureConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerFeatureConfig>  {

    String TABLE_NAME = " tb_broker_feature_config ";

    @Update("update " + TABLE_NAME + " set status=#{status} where broker_id=#{brokerId} and status=#{oldStatus}")
    int deleteConfig(@Param("status") Integer status, @Param("oldStatus") Integer oldStatus, @Param("brokerId") Long brokerId);

    @Select("select * from " + TABLE_NAME + " where broker_id=#{brokerId} and status= 1 order by locale,rank")
    List<BrokerFeatureConfig> getAvailableConfigs(@Param("brokerId") Long brokerId);
}
