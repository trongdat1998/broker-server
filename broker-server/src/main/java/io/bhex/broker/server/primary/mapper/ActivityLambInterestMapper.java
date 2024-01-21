package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLambInterest;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/5/29 11:45 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLambInterestMapper extends Mapper<ActivityLambInterest> {

}
