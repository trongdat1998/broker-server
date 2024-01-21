package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcThirdPartyDisclaimer;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


@Mapper
public interface OtcThirdPartyDisclaimerMapper extends tk.mybatis.mapper.common.Mapper<OtcThirdPartyDisclaimer> {
    @Select("SELECT * FROM tb_otc_third_party_disclaimer WHERE org_id=#{orgId} AND third_party_id=#{thirdPartyId} AND language=#{language};")
    OtcThirdPartyDisclaimer getByOrgId(@Param("orgId") Long orgId,
                                       @Param("thirdPartyId") Long thirdPartyId,
                                       @Param("language") String language);

}
