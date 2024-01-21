package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.grpc.server.service.po.BannerPO;
import io.bhex.broker.server.model.Banner;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 28/08/2018 3:22 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface BannerMapper extends Mapper<Banner> {

    String TABLE_NAME = " tb_banner ";

    String DETAIL_TABLE_NAME = " tb_banner_detail ";

    String COLUMNS = "id, broker_id, admin_user_id, rank, begin_at, end_at, remark, platform,banner_position, status, created_at";

    String DETAIL_COLUMNS = "b.id, b.broker_id, d.admin_user_id, b.rank, b.begin_at, b.end_at, b.remark, b.platform, b.banner_position, b.status, d.created_at, d.title, d.image_url, d.h5_image_url, d.content, d.remark, d.locale, d.type, d.page_url, d.is_default";

    @SelectProvider(type = BannerSqlProvider.class, method = "count")
    int countByBrokerId(@Param("brokerId") Long brokerId, @Param("platform") Integer platform, @Param("bannerPosition") Integer bannerPosition);

    @SelectProvider(type = BannerSqlProvider.class, method = "queryBanner")
    List<Banner> listBanner(@Param("fromindex") Integer fromindex, @Param("endindex") Integer endindex,
                            @Param("brokerId") Long brokerId, @Param("platform") Integer platform, @Param("bannerPosition") Integer bannerPosition);

    @Select("SELECT " + DETAIL_COLUMNS + " FROM " + TABLE_NAME + " as b join " + DETAIL_TABLE_NAME
            + " as d on b.id=d.banner_id WHERE b.broker_id=#{brokerId} and b.begin_at<#{currentTimeMillis} and b.end_at>#{currentTimeMillis} and d.status=1 order by b.rank desc")
    List<BannerPO> queryOrgBanners(@Param("brokerId") Long brokerId, @Param("currentTimeMillis") Long currentTimeMillis);

    @Select("SELECT " + DETAIL_COLUMNS + " FROM " + TABLE_NAME + " as b join " + DETAIL_TABLE_NAME
            + " as d on b.id=d.banner_id WHERE b.broker_id = #{brokerId} and b.platform = #{platform} and b.begin_at<#{currentTimeMillis} "
            + "and b.end_at>#{currentTimeMillis} and d.locale=#{locale} and d.status=1 order by b.rank desc limit #{size}")
    List<BannerPO> listBannerByLocal(@Param("size") Integer size, @Param("brokerId") Long brokerId,
                                     @Param("locale") String locale, @Param("platform") Integer platform, @Param("currentTimeMillis") Long currentTimeMillis);

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE id = #{bannerId} and broker_id=#{brokerId}")
    Banner getBannerById(@Param("brokerId") Long brokerId, @Param("bannerId") Long bannerId);

    @Update("update " + TABLE_NAME + " set status=#{status}, admin_user_id=#{adminUserId} where id=#{id} and broker_id=#{brokerId}")
    int pushBanner(@Param("id") Long id, @Param("status") Integer status, @Param("brokerId") Long brokerId, @Param("adminUserId") Long adminUserId);

    @Update("UPDATE " + TABLE_NAME + " set status=2, admin_user_id=#{adminUserId} where id=#{bannerId}")
    int deleteByBannerId(@Param("bannerId") Long bannerId, @Param("adminUserId") Long adminUserId);
}
