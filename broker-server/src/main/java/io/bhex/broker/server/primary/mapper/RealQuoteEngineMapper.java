package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.RealQuoteEngineAddress;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;


/**
 * @author wangshouchao
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface RealQuoteEngineMapper extends Mapper<RealQuoteEngineAddress> {

}
