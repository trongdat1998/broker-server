package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.RptMarginPool;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-01-11 10:07
 */
@Mapper
public interface RptMarginPoolMapper extends tk.mybatis.mapper.common.Mapper<RptMarginPool> {

    @Select("select * from tb_rpt_margin_pool where org_id = #{orgId} and token_id = #{tokenId} ;")
    RptMarginPool getMarginPoolRecord(@Param("orgId") Long orgId,@Param("tokenId") String tokenId);

}
