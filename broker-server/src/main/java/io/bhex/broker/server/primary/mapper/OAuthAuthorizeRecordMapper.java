package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OAuthAuthorizeRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface OAuthAuthorizeRecordMapper extends tk.mybatis.mapper.common.Mapper<OAuthAuthorizeRecord> {

    @Select("SELECT * FROM tb_oauth_authorize_record WHERE org_id=#{orgId} AND request_id=#{requestId} FOR UPDATE")
    OAuthAuthorizeRecord getByOAuthRequestIdForUpdate(@Param("orgId") Long orgId, @Param("requestId") String requestId);

}
