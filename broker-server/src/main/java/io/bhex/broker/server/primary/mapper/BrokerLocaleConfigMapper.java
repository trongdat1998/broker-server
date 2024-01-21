package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerLocaleConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 11:46 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Mapper
@Component
public interface BrokerLocaleConfigMapper extends tk.mybatis.mapper.common.Mapper<BrokerLocaleConfig> {

    String TABLE_NAME = " tb_broker_locale_config ";

    @Update("update " + TABLE_NAME + " set status=#{status} where broker_id=#{brokerId} and status=#{oldStatus}")
    int deleteConfig(@Param("status") Integer status, @Param("oldStatus") Integer oldStatus, @Param("brokerId") Long brokerId);

    @Select("select feature_title from " + TABLE_NAME + " where status = 1 and broker_id=#{brokerId} and locale = #{locale} limit 1")
    String getFeatureTitle(@Param("brokerId") Long brokerId, @Param("locale") String locale);
}
