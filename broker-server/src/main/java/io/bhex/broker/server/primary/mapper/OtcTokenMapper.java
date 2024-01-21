package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcToken;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcTokenMapper extends Mapper<OtcToken> {

    @Select("select *from tb_otc_token order by sequence desc")
    List<OtcToken> queryAllTokenList();

    @Select("select *from tb_otc_token where org_id=#{orgId} order by sequence desc ")
    List<OtcToken> queryTokenListByOrgId(@Param("orgId")Long orgId);
}
