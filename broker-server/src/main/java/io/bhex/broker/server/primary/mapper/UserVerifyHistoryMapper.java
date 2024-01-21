package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.UserVerifyHistory;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 24/08/2018 2:59 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Mapper
public interface UserVerifyHistoryMapper extends tk.mybatis.mapper.common.Mapper<UserVerifyHistory> {

    String TABLE_NAME = " tb_user_verify_history ";

    String COLUMNS = "id, user_verify_id, verify_reason_id, remark, admin_user_id, info_upload_at, status, created_at, updated_at";

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE user_verify_id = #{userVerifyId}")
    List<UserVerifyHistory> listAll(@Param("brokerId") Long brokerId, @Param("userVerifyId") Long userVerifyId);

    @Select("select "  + COLUMNS + " FROM " + TABLE_NAME +  " WHERE status=2 and user_verify_id = #{userVerifyId} limit 1")
    UserVerifyHistory getPassedVerify(@Param("userVerifyId") Long userVerifyId);

    @Select("select "  + COLUMNS + " FROM " + TABLE_NAME +  " WHERE status=3 and user_verify_id = #{userVerifyId} order by id desc limit 1")
    UserVerifyHistory getRejectedVerify(@Param("userVerifyId") Long userVerifyId);

    @Select("select "  + COLUMNS + " from " + TABLE_NAME + " where id > #{lastId} order by id asc limit #{limit}")
    List<UserVerifyHistory> getUserVerifyHistories(@Param("lastId") Long lastId, @Param("limit") Integer limit);
}
