package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.NewsDetails;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface NewsDetailsMapper extends tk.mybatis.mapper.common.Mapper<NewsDetails> {

}
