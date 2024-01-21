package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.LockBalanceLog;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Mapper
@Component
public interface LockBalanceLogMapper extends tk.mybatis.mapper.common.Mapper<LockBalanceLog> {

    @Update("update tb_lock_balance_log set status = #{status},update_time = now() where broker_id = #{brokerId} and user_id=#{userId} and client_order_id = #{clientOrderId}")
    int updateUserLockBalanceLogStatus(@Param("brokerId") Long brokerId, @Param("userId") Long userId, @Param("clientOrderId") Long clientOrderId, @Param("status") Integer status);


    @Select("select *from tb_lock_balance_log where id = #{id} for update")
    LockBalanceLog queryLockBalanceLogByIdLock(@Param("id") Long id);

    @Update("update tb_lock_balance_log set unlock_amount = unlock_amount + #{amount},last_amount = last_amount - #{amount},update_time = now() where id = #{id}")
    int updateUserUnLockBalanceLog(@Param("id") Long id, @Param("amount") BigDecimal amount);

    @Select("select *from tb_lock_balance_log where broker_id = #{orgId} and user_id=#{userId} and if (#{type} = 1, type != 4 , type = #{type} )  and status = 1 order by create_time desc limit #{start},#{limit}")
    List<LockBalanceLog> queryLockBalanceLogListByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("start") Integer start, @Param("limit") Integer limit, @Param("type") Integer type);

    @Select("select *from tb_lock_balance_log where broker_id = #{orgId} and if (#{type} = 1, type != 4 , type = #{type} ) and status = 1 order by create_time desc limit #{start},#{limit}")
    List<LockBalanceLog> queryLockBalanceLogList(@Param("orgId") Long orgId, @Param("start") Integer start, @Param("limit") Integer limit, @Param("type") Integer type);
}
