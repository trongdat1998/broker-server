package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MarginActivity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface MarginActivityMapper extends tk.mybatis.mapper.common.Mapper<MarginActivity> {

    @Select("select * from tb_margin_activity where id = #{id} and org_id = #{orgId};")
    MarginActivity getByOrgIdAndId(@Param("orgId") Long orgId, @Param("id") Long id);
}
