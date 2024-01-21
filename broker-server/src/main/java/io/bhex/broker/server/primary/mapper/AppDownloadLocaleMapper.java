package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AppDownloadLocale;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @Description:
 * @Date: 2019/8/11 下午6:38
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface AppDownloadLocaleMapper extends Mapper<AppDownloadLocale> {

    String TABLE_NAME = " tb_app_download_locale ";

    String COLUMNS = " * ";

    @Update("update " + TABLE_NAME + " set status = 0 where broker_id=#{brokerId} and status=1")
    int deleteByBrokerId(@Param("brokerId") Long brokerId);


    @Select("select * from " + TABLE_NAME + " where broker_id=#{brokerId} and status = 1 ")
    List<AppDownloadLocale> getRecords(@Param("brokerId") Long brokerId);

    @Select("select * from " + TABLE_NAME + " where status = 1 ")
    List<AppDownloadLocale> getAllAvailableRecords();

    @Select("select * from " + TABLE_NAME + " where broker_id=#{brokerId} and language=#{language} and status = 1 ")
    AppDownloadLocale getAvailableRecord(@Param("brokerId") Long brokerId, @Param("language") String language);
}
