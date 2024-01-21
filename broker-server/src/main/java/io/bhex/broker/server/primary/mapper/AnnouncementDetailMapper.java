package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AnnouncementDetail;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 27/08/2018 2:51 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface AnnouncementDetailMapper  extends Mapper<AnnouncementDetail> {

    String TABLE_NAME = " tb_announcement_detail ";

    String COLUMNS = "id, announcement_id, admin_user_id, title, content, locale, type, page_url, is_default, status, created_at";

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE announcement_id = #{announcementId} and status=1")
    List<AnnouncementDetail> getDetailByAnnouncementId(@Param("announcementId") Long announcementId);

    @Select(" <script> "
            + " SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE announcement_id in "
            +   " <foreach item='item' index='index' collection='announcementIds' open='(' separator=',' close=')'> "
            +       " #{item} "
            +   " </foreach> "
            +   " and status=1"
            + " </script> ")
    List<AnnouncementDetail> listDetailByAnnouncementIds(@Param("announcementIds") List<Long> announcementIds);

    @Update("UPDATE " + TABLE_NAME + " set status=2, admin_user_id=#{adminUserId} where announcement_id=#{announcementId}")
    int deleteByAnnouncementId(@Param("announcementId") Long announcementId, @Param("adminUserId") Long adminUserId);
}
