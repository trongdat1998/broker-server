package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.UserContract;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/1/31 5:51 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
@Component
public interface UserContractMapper extends Mapper<UserContract> {

}
