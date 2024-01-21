package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.LockBalanceLog;
import io.bhex.broker.server.model.UnlockBalanceLog;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

@Mapper
@Component
public interface UnlockBalanceLogMapper extends tk.mybatis.mapper.common.Mapper<UnlockBalanceLog> {

}
