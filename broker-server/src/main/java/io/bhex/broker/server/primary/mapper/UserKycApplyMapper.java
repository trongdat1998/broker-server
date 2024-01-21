package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.UserKycApply;
import io.bhex.broker.server.model.UserVerifyHistory;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Mapper
public interface UserKycApplyMapper extends tk.mybatis.mapper.common.Mapper<UserKycApply> {

    @Update({"UPDATE tb_user_kyc_apply SET verify_reason_id=#{verifyReasonId}, verify_status=#{verifyStatus}, verify_message=#{verifyMessage}, " +
            "updated=#{now} WHERE user_id=#{userId} AND org_id=#{orgId} and id = #{kycApplyId}"})
    int updateVerifyStatus(@Param("kycApplyId") Long kycApplyId, @Param("userId") Long userId, @Param("orgId") Long orgId, @Param("verifyReasonId") Long verifReasonId,
                           @Param("verifyStatus") Integer verifyStatus, @Param("verifyMessage") String verifyMessage, @Param("now") long now);

    @Select("select id,user_id,org_id,verify_status,updated from tb_user_kyc_apply where updated > #{lastUpdated} and kyc_level > 10 and verify_status in (2,3) order by updated asc limit #{limit}")
    List<UserKycApply> getUserVerifyHistories(@Param("lastUpdated") Long lastUpdated, @Param("limit") Integer limit);
}
