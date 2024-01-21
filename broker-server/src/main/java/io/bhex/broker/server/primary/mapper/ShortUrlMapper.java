package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ShortUrl;
import tk.mybatis.mapper.common.Mapper;

/**
 * 短链接mapper
 *
 * @author lizhen
 * @date 2018-11-27
 */
@org.apache.ibatis.annotations.Mapper
public interface ShortUrlMapper extends Mapper<ShortUrl> {
}
