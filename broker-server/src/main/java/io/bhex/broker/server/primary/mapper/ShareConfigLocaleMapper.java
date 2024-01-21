package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ShareConfigLocale;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.primary.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/6/27 5:58 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ShareConfigLocaleMapper extends Mapper<ShareConfigLocale> {

    String TABLE_NAME = " tb_share_config_locale ";

    String COLUMNS = " * ";

    @Update("update " + TABLE_NAME + " set status = 0 where broker_id=#{brokerId} and status=1")
    int deleteByBrokerId(@Param("brokerId") Long brokerId);
}
