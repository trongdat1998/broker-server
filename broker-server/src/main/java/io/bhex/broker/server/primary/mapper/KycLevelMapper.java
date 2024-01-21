package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.KycLevel;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Component;

@Component
@Mapper
public interface KycLevelMapper extends tk.mybatis.mapper.common.Mapper<KycLevel> {

}