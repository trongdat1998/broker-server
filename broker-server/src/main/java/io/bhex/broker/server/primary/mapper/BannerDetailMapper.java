package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BannerDetail;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 28/08/2018 3:51 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface BannerDetailMapper extends Mapper<BannerDetail> {

    String TABLE_NAME = " tb_banner_detail ";

    String COLUMNS = "id, admin_user_id, broker_id, banner_id, title, content, image_url, h5_image_url,remark, locale, is_default, type, page_url, status, created_at";

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE banner_id = #{bannerId} and status=1")
    List<BannerDetail> getDetailByBannerId(@Param("bannerId") Long bannerId);

    @Select(" <script> "
            + " SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE banner_id in "
            +   " <foreach item='item' index='index' collection='bannerIds' open='(' separator=',' close=')'> "
            +       " #{item} "
            +   " </foreach> "
            +   " and status = 1"
            + " </script> ")
    List<BannerDetail> listDetailByBannerIds(@Param("bannerIds") List<Long> bannerIds);

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE banner_id = #{bannerId} and locale=#{locale} and status=1")
    BannerDetail getDetailByBannerIdAndLocale(@Param("bannerId") Long bannerId, @Param("locale") String locale);

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE id = #{bannerDetailId} and status=1")
    BannerDetail getById(@Param("bannerDetailId") Long bannerDetailId);

    @Update("UPDATE " + TABLE_NAME + " set status=2, admin_user_id=#{adminUserId} where banner_id=#{bannerId}")
    int deleteByBannerId(@Param("bannerId") Long bannerId, @Param("adminUserId") Long adminUserId);
}
