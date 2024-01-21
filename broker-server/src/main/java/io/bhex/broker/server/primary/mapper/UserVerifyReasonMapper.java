package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.grpc.server.service.po.UserVerifyReasonPO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 24/08/2018 3:00 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Mapper
public interface UserVerifyReasonMapper {

    String TABLE_NAME = " tb_user_verify_reason ";

    String DETAIL_TABLE_NAME = " tb_user_verify_reason_detail ";

    String COLUMNS = "id, user_id, nationality, first_name, created, updated";

    String DETAIL_COLUMNS = "r.id, d.reason, d.locale";

    @Select("SELECT " + DETAIL_COLUMNS + " FROM " + TABLE_NAME + " as r join " + DETAIL_TABLE_NAME + " as d on r.id=d.verify_reason_id WHERE d.locale = #{locale} and r.status = 1")
    List<UserVerifyReasonPO> listAll(@Param("locale") String locale);

    @Select("SELECT id, reason, locale FROM " + DETAIL_TABLE_NAME + " WHERE locale = #{locale} and verify_reason_id = #{verifyReasonId} limit 1")
    UserVerifyReasonPO getVerifyReason(@Param("verifyReasonId") int verifyReasonId, @Param("locale") String locale);
}
