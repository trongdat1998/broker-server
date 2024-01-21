package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcWhiteList;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

/**
 * @author lizhen
 * @date 2018-11-13
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcWhiteListMapper extends Mapper<OtcWhiteList> {

    @Select("select *from tb_otc_white_list where org_id = #{orgId} and user_id=#{userId} limit 0,1")
    OtcWhiteList queryByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);
}
