package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcThirdParty;
import io.bhex.broker.server.model.OtcThirdPartyChain;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface OtcThirdPartyChainMapper extends tk.mybatis.mapper.common.Mapper<OtcThirdPartyChain> {

    @Select("SELECT * FROM tb_otc_third_party_chain WHERE status = #{status};")
    List<OtcThirdPartyChain> queryAllRecord(@Param("status") Integer status);
}
