package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.grpc.server.service.po.AnnouncementPO;
import io.bhex.broker.server.model.Announcement;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 27/08/2018 11:06 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
@Component
public interface AnnouncementMapper extends Mapper<Announcement> {

    String TABLE_NAME = " tb_announcement ";

    String DETAIL_TABLE_NAME = " tb_announcement_detail ";

    String COLUMNS = "id, broker_id, admin_user_id, status, platform, rank, begin_at, channel, end_at, created_at";

    String DETAIL_COLUMNS = "a.id, a.broker_id, a.admin_user_id, a.status, a.platform, a.rank, a.begin_at, a.end_at, a.created_at, d.title, d.content, d.type, d.page_url, d.locale, d.is_default, a.channel";

    @SelectProvider(type = AnnouncementSqlProvider.class, method = "count")
    int countByBrokerId(@Param("brokerId") Long brokerId, @Param("platform") Integer platform);

    @SelectProvider(type = AnnouncementSqlProvider.class, method = "queryAnnouncement")
    List<Announcement> listAnnouncement(@Param("fromindex") Integer fromindex, @Param("endindex") Integer endindex,
                                        @Param("brokerId") Long brokerId, @Param("platform") Integer platform);

    @Select("SELECT " + DETAIL_COLUMNS + " FROM " + TABLE_NAME + " as a join " + DETAIL_TABLE_NAME + " as d on a.id=d.announcement_id "
            + "WHERE a.broker_id=#{brokerId} and a.begin_at<#{currentTimeMillis} and a.end_at>#{currentTimeMillis} and d.status=1")
    List<AnnouncementPO> queryOrgAnnouncements(@Param("brokerId") Long brokerId, @Param("currentTimeMillis") Long currentTimeMillis);

    @Select("SELECT " + DETAIL_COLUMNS + " FROM " + TABLE_NAME + " as a join " + DETAIL_TABLE_NAME + " as d on a.id=d.announcement_id "
            + "WHERE b.broker_id = #{brokerId} and a.platform = #{platform} and a.begin_at<#{currentTimeMillis} and a.end_at>#{currentTimeMillis} "
            + "and d.locale=#{locale} and d.status=1 order by a.rank desc limit #{size}")
    List<AnnouncementPO> listAnnouncementByLocale(@Param("size") Integer size, @Param("brokerId") Long brokerId, @Param("locale") String locale,
                                                  @Param("platform") Integer platform, @Param("currentTimeMillis") Long currentTimeMillis);

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE id = #{announcementId} and broker_id=#{brokerId}")
    Announcement getAnnouncementById(@Param("brokerId") Long brokerId, @Param("announcementId") Long announcementId);

    @Update("update " + TABLE_NAME + " set status=#{status}, admin_user_id=#{adminUserId} where id=#{id} and broker_id=#{brokerId}")
    int pushAnnouncement(@Param("id") Long id, @Param("status") Integer status, @Param("brokerId") Long brokerId, @Param("adminUserId") Long adminUserId);

    @Update("UPDATE " + TABLE_NAME + " set status=2, admin_user_id=#{adminUserId} where id=#{announcementId}")
    int deleteByAnnouncementId(@Param("announcementId") Long announcementId, @Param("adminUserId") Long adminUserId);

}
