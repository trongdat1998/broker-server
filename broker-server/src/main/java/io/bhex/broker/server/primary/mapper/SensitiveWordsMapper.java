package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.SensitiveWords;
import tk.mybatis.mapper.common.Mapper;

/**
 * @author lizhen
 * @date 2018-12-03
 */
@org.apache.ibatis.annotations.Mapper
public interface SensitiveWordsMapper extends Mapper<SensitiveWords> {
}
