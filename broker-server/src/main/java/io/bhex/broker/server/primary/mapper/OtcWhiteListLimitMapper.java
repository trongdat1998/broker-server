package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import io.bhex.broker.server.model.OtcWhiteListLimit;
import tk.mybatis.mapper.common.Mapper;

@org.apache.ibatis.annotations.Mapper
public interface OtcWhiteListLimitMapper extends Mapper<OtcWhiteListLimit> {
    @Select("select *from tb_otc_white_list_limit where org_id= #{orgId} and status = 1")
    OtcWhiteListLimit query(@Param("orgId") Long orgId);
}
