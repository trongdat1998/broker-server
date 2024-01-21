package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FindPwdRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface FindPwdRecordMapper extends tk.mybatis.mapper.common.Mapper<FindPwdRecord> {

    @Update("UPDATE tb_find_pwd_record set auth_order_id=#{authOrderId}, auth_request_count = auth_request_count + 1 WHERE id = #{id}")
    int updateAuthRequestInfo(@Param("id") Long id, @Param("authOrderId") Long authOrderId);

}
