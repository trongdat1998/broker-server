package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceBatchTransfer;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface BalanceBatchTransferMapper extends tk.mybatis.mapper.common.Mapper<BalanceBatchTransfer>, InsertListMapper<BalanceBatchTransfer> {


}
