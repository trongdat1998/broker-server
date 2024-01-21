package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceInterestData;
import org.apache.ibatis.annotations.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface FinanceInterestDataMapper extends tk.mybatis.mapper.common.Mapper<FinanceInterestData>, InsertListMapper<FinanceInterestData> {

}
