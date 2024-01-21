package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ThirdPartyAppUserAuthorize;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface ThirdPartyAppUserAuthorizeMapper extends tk.mybatis.mapper.common.Mapper<ThirdPartyAppUserAuthorize> {

    @Select("SELECT * FROM tb_third_party_app_user_authorize WHERE org_id=#{orgId} AND oauth_code=#{oauthCode} FOR UPDATE")
    ThirdPartyAppUserAuthorize getByOrgIdAndOAuthCodeForUpdate(@Param("orgId") Long orgId, @Param("oauthCode") String oauthCode);

    @Select("SELECT * FROM tb_third_party_app_user_authorize WHERE org_id=#{orgId} AND access_token=#{accessToken} FOR UPDATE")
    ThirdPartyAppUserAuthorize getByOrgIdAndAccessToken(@Param("orgId") Long orgId, @Param("accessToken") String accessToken);

    @Update("UPDATE tb_third_party_app_user_authorize SET token_status=#{tokenStatus} WHERE org_id=#{orgId} AND user_id=#{userId}")
    int disableUserTokenStatus(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("tokenStatus") int tokenStatus);

    @Update("UPDATE tb_third_party_app_user_authorize SET token_status=#{tokenStatus} WHERE org_id=#{orgId} AND third_party_app_id=#{thirdPartyAppId}")
    int disableAppTokenStatus(@Param("orgId") Long orgId, @Param("thirdPartyAppId") Long thirdPartyAppId, @Param("tokenStatus") int tokenStatus);

}
