package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ThirdPartyUser;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


@Mapper
public interface ThirdPartyUserMapper extends tk.mybatis.mapper.common.Mapper<ThirdPartyUser> {

    @Select("select *from tb_third_party_user where org_id = #{orgId} and master_user_id = #{masterUserId} and third_user_id = #{thirdUseId}")
    ThirdPartyUser getByMasterUserIdAndThirdUserId(@Param("orgId") Long orgId, @Param("masterUserId") Long masterUserId, @Param("thirdUseId") String thirdUseId);

    @Select("select *from tb_third_party_user where org_id = #{orgId} and master_user_id = #{masterUserId} and user_id = #{userId}")
    ThirdPartyUser getByMasterUserIdAndUserId(@Param("orgId") Long orgId, @Param("masterUserId") Long masterUserId, @Param("userId") Long userId);

    @Select("select *from tb_third_party_user where org_id = #{orgId} and user_id = #{userId}")
    ThirdPartyUser getByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

}
