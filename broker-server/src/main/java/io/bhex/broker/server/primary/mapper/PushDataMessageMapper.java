package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.DataPushMessage;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

/**
 *
 */
@Mapper
public interface PushDataMessageMapper extends tk.mybatis.mapper.common.Mapper<DataPushMessage>, InsertListMapper<DataPushMessage> {

    @Select("select *from tb_data_push_message where org_id = #{orgId} and message_status = 0 and message_type != 'FUTURES.MESSAGE' and retry_count < #{retryCount} limit 0,50")
    List<DataPushMessage> queryMessageList(@Param("orgId") Long orgId, @Param("retryCount") Integer retryCount);

    @Update("update tb_data_push_message set message_status = 1 where client_req_id = #{clientReqId} and message_status = 0")
    int updateMessageStatus(@Param("clientReqId") String clientReqId);

    @Update("update tb_data_push_message set retry_count = retry_count + 1 where client_req_id = #{clientReqId}")
    int addRetryCount(@Param("clientReqId") String clientReqId);
}
