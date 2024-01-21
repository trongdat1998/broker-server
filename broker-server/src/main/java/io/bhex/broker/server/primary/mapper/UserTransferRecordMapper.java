package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.UserTransferRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface UserTransferRecordMapper extends tk.mybatis.mapper.common.Mapper<UserTransferRecord> {

    @Select(" SELECT * FROM tb_user_transfer_record WHERE org_id=#{orgId} AND user_id=#{userId} AND client_order_id=#{clientOrderId}")
    UserTransferRecord getByClientOrderIdAndSourceUserId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("clientOrderId") String clientOrderId);

}
