package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AutoAirdrop;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 30/11/2018 3:13 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface AutoAirdropMapper extends Mapper<AutoAirdrop> {

}
