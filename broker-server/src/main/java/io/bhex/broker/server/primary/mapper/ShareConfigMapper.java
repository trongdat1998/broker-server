package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.grpc.server.service.po.ShareConfigPO;
import io.bhex.broker.server.model.ShareConfig;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.primary.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/6/27 5:58 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ShareConfigMapper extends Mapper<ShareConfig> {

    String TABLE_NAME = " tb_share_config ";

    String COLUMNS = " * ";

    String DETAIL_TABLE_NAME = " tb_share_config_locale ";

    String DETAIL_COLUMNS = "s.broker_id, s.logo_url, s.watermark_image_url, l.title, l.description, l.download_url, l.language";

    @Update("update " + TABLE_NAME + " set status = 0 where broker_id=#{brokerId} and status=1")
    int deleteByBrokerId(@Param("brokerId") Long brokerId);

    @Select("SELECT " + DETAIL_COLUMNS + " FROM " + TABLE_NAME + " as s join " + DETAIL_TABLE_NAME
            + " as l on s.broker_id=l.broker_id WHERE s.status=1 and l.status=1")
    List<ShareConfigPO> listAllShareConfig();

    @Select("SELECT " + DETAIL_COLUMNS + " FROM " + TABLE_NAME + " as s join " + DETAIL_TABLE_NAME
            + " as l on s.broker_id=l.broker_id WHERE s.broker_id=#{orgId} and s.status=1 and l.status=1")
    List<ShareConfigPO> listShareConfigByOrgId(@Param(value = "orgId") Long orgId);
}
