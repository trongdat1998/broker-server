package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterestLocal;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/6/5 4:45 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestLocalMapper extends Mapper<ActivityLockInterestLocal> {

}
