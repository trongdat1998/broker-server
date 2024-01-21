package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcThirdParty;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface OtcThirdPartyMapper extends tk.mybatis.mapper.common.Mapper<OtcThirdParty> {
    @Select("SELECT * FROM tb_otc_third_party WHERE org_id=#{orgId} AND side=#{side};")
    List<OtcThirdParty> queryListByOrgId(@Param("orgId") Long orgId, @Param("side") Integer side);

    @Select("SELECT * FROM tb_otc_third_party WHERE org_id=#{orgId} AND side=#{side} AND third_party_id=#{thirdPartyId};")
    OtcThirdParty getByThirdPartyId(@Param("orgId") Long orgId,
                                    @Param("thirdPartyId") Long thirdPartyId,
                                    @Param("side") Integer side);

    @Select("SELECT * FROM tb_otc_third_party WHERE bind_org_id=#{bindOrgId} AND bind_user_id=#{bindUserId} LIMIT 1;")
    OtcThirdParty getByUser(@Param("bindOrgId") Long bindOrgId, @Param("bindUserId") Long bindUserId);

}
