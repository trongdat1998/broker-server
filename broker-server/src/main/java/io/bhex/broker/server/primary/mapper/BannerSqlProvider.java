package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 04/09/2018 6:10 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class BannerSqlProvider {

    public String count(Map<String, Object> parameter) {

        Integer platform = (Integer) parameter.get("platform");
        Integer bannerPosition = (Integer) parameter.get("bannerPosition");
        return new SQL() {
            {
                SELECT("count(*)").FROM(BannerMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("status=1");
                if (platform != null && platform != 0) {
                    WHERE("platform = #{platform}");
                }
                if (bannerPosition != null && bannerPosition != 0) {
                    WHERE("banner_position = #{bannerPosition}");
                }
            }
        }.toString();
    }

    public String queryBanner(Map<String, Object> parameter) {

        Integer platform = (Integer) parameter.get("platform");
        Integer bannerPosition = (Integer) parameter.get("bannerPosition");
        return new SQL() {
            {
                SELECT(BannerMapper.COLUMNS).FROM(BannerMapper.TABLE_NAME);
                WHERE("broker_id = #{brokerId}");
                WHERE("status=1");
                if (platform != null && platform != 0) {
                    WHERE("platform = #{platform}");
                }
                if (bannerPosition != null && bannerPosition != 0) {
                    WHERE("banner_position = #{bannerPosition}");
                }
                ORDER_BY("id DESC limit #{fromindex}, #{endindex}");
            }
        }.toString();
    }

}
