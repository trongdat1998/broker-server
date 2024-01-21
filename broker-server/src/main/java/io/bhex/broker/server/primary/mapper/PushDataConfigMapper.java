package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.DataPushConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 *
 */
@Mapper
public interface PushDataConfigMapper extends tk.mybatis.mapper.common.Mapper<DataPushConfig> {

    @Select("select * from tb_data_push_config where status = 1")
    List<DataPushConfig> queryAllOpen();

    @Select("select * from tb_data_push_config where org_id = #{orgId} and status = 1")
    DataPushConfig queryConfigOpenOrgId(@Param("orgId") Long orgId);

}
