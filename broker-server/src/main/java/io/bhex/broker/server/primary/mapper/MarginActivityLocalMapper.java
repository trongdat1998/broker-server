package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MarginActivityLocal;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface MarginActivityLocalMapper extends tk.mybatis.mapper.common.Mapper<MarginActivityLocal> {

    @Select("select * from tb_margin_activity_local where activity_id = #{activityId} and language = #{language} and status = 1 limit 1;")
    MarginActivityLocal getByActivityIdAndLanguage(@Param("activityId") Long activityId, @Param("language") String language);
}
