package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.NoticeTemplateInitData;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 27/11/2018 4:06 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface NoticeTemplateInitDataMapper extends Mapper<NoticeTemplateInitData> {

}
