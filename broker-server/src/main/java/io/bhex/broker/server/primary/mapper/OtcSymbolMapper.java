package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcSymbol;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Mapper
public interface OtcSymbolMapper extends tk.mybatis.mapper.common.Mapper<OtcSymbol> {
}
