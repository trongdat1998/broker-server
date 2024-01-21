package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerTaskConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

@Mapper
@Component
public interface BrokerTaskConfigMapper  extends tk.mybatis.mapper.common.Mapper<BrokerTaskConfig> {

    @Update("update tb_broker_task_config set status = #{newStatus} where id = #{id} and status = #{oldStatus}")
    int updateStatus(@Param("id") Long id, @Param("newStatus") int newStatus, @Param("oldStatus") String oldStatus);

    @Select("select * from tb_broker_task_config where id = #{id} for update")
    BrokerTaskConfig selectForUpdate(@Param("id") Long id);
}
