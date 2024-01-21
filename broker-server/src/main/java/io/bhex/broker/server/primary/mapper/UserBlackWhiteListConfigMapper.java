package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.domain.UserBlackWhiteListConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface UserBlackWhiteListConfigMapper  extends tk.mybatis.mapper.common.Mapper<UserBlackWhiteListConfig> {

    @Select("SELECT * FROM tb_user_black_white_list_config WHERE org_id=#{orgId} AND user_id=#{userId} "
            + "AND list_type=#{listType} AND bw_type=#{bwType} "
            + " AND #{now} > start_time AND end_time >= #{now} AND status=1 ORDER BY id DESC LIMIT 1")
    UserBlackWhiteListConfig getOneConfig(@Param("orgId") Long orgId, @Param("userId") Long userId,
                                       @Param("listType") Integer listType, @Param("bwType") Integer bwType,
                                          @Param("now") Long now);

    @Update(("update tb_user_black_white_list_config set status = 0,updated=#{now}  where org_id=#{orgId} and status = 1 and list_type=#{listType} AND bw_type=#{bwType} "))
    int deleteConfigs(@Param("orgId") Long orgId, @Param("listType") Integer listType, @Param("bwType") Integer bwType,
                                          @Param("now") Long now);
}
