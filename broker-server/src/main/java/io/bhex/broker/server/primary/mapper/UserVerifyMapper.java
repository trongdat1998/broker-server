/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/7/8
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.UserIdCheckLog;
import io.bhex.broker.server.model.UserVerify;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Mapper
public interface UserVerifyMapper extends tk.mybatis.mapper.common.Mapper<UserVerify> {

    String TABLE_NAME = " tb_user_verify ";

    String COLUMNS = "id, user_id, org_id, kyc_level, nationality, country_code, first_name, second_name, gender, "
            + "card_type, card_no, card_no_hash, card_front_url, card_back_url, card_hand_url, "
            + "face_photo_url, face_video_url, video_url, data_encrypt, data_secret, "
            + "passed_id_check, verify_status, verify_reason_id, kyc_apply_id, created, updated";

    @SelectProvider(type = UserVerifySqlProvider.class, method = "getByUserId")
    UserVerify getByUserId(Long userId);

    @Deprecated
    @SelectProvider(type = UserVerifySqlProvider.class, method = "getByOrgIdAndCardNo")
    List<UserVerify> queryByOrgIdAndCardNo(@Param("orgId") Long orgId, @Param("cardNo") String cardNo);

    @SelectProvider(type = UserVerifySqlProvider.class, method = "getByOrgIdAndCardNoHash")
    List<UserVerify> queryByOrgIdAndCardNoHash(@Param("orgId") Long orgId, @Param("cardNoHash") String cardNoHash);

    @InsertProvider(type = UserVerifySqlProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insertRecord(UserVerify userVerify);

    @UpdateProvider(type = UserVerifySqlProvider.class, method = "update")
    int update(UserVerify userVerify);

    //admin
    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE id=#{id}")
    UserVerify getById(@Param("id") Long id);

//    @Select("SELECT count(id) FROM " + TABLE_NAME + " WHERE org_id = #{brokerId} and verify_status = ${verifyStatus}")
//    int countByBrokerId(@Param("brokerId") Long brokerId, @Param("verifyStatus") Integer verifyStatus);

    //@Select("SELECT count(id) FROM " + TABLE_NAME + " WHERE org_id = #{brokerId} and verify_status = ${verifyStatus}")
    @SelectProvider(type = UserVerifySqlProvider.class, method = "countByBrokerId")
    int countByBrokerId(@Param("userId") Long userId, @Param("brokerId") Long brokerId, @Param("verifyStatus") Integer verifyStatus,
                        @Param("nationality") Integer nationality, @Param("cardType") Integer cardType, @Param("startTime") Long startTime, @Param("endTime") Long endTime);

    @SelectProvider(type = UserVerifySqlProvider.class, method = "queryUserVerify")
    List<UserVerify> queryVerifyInfo(@Param("fromindex") Integer fromindex, @Param("endindex") Integer endindex, @Param("userId") Long userId,
                                     @Param("brokerId") Long brokerId, @Param("verifyStatus") Integer verifyStatus,
                                     @Param("nationality") Integer nationality, @Param("cardType") Integer cardType, @Param("startTime") Long startTime, @Param("endTime") Long endTime);

    @Update("update " + TABLE_NAME + " set verify_status=#{verifyStatus}, verify_reason_id=#{reasonId}, updated=unix_timestamp() * 1000 where id=#{userVerifyId} and org_id=#{brokerId}")
    int updateVerifyUser(@Param("userVerifyId") Long userVerifyId, @Param("brokerId") Long brokerId, @Param("verifyStatus") Integer verifyStatus, @Param("reasonId") Long reasonId);

    @Update("UPDATE " + TABLE_NAME + " SET verify_status=#{verifyStatus}, verify_reason_id=#{reasonId}, updated=unix_timestamp() * 1000 WHERE user_id=#{userId} AND org_id=#{orgId} and verify_status = 1")
    int updateVerifyStatusByUserId(@Param("userId") Long userId, @Param("orgId") Long orgId, @Param("verifyStatus") Integer verifyStatus, @Param("reasonId") Long reasonId);

    @Select("select * from " + TABLE_NAME + " where id > #{lastId} order by id asc limit #{limit}")
    List<UserVerify> getUserVerifyList(@Param("lastId") Long lastVerifyId, @Param("limit") Integer limit);

    @Select("SELECT id, user_id, org_id FROM " + TABLE_NAME + " WHERE verify_status=2")
    List<UserVerify> getAllKycPassUserList();

    @Select("SELECT * FROM " + TABLE_NAME + " WHERE user_id=#{userId} and verify_status=2")
    UserVerify getKycPassUserByUserId(@Param("userId") Long userId);

    @Insert("INSERT INTO tb_user_id_check_log(id, org_id, user_id, `name`, id_no, response, created) "
            + "VALUES(#{id}, #{orgId}, #{userId}, #{name}, #{idNo}, #{response}, #{created})")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insertUserIdCheckLog(UserIdCheckLog userIdCheckLog);

    @Select({"<script>"
            , "SELECT id, user_id, first_name, second_name, verify_status "
            , "FROM tb_user_verify "
            , "WHERE org_id=#{orgId} "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND id &gt; #{lastId}</if> "
            , "<if test=\"startTime != null and startTime &gt; 0\">AND (created &gt;= #{startTime} OR updated &gt;= #{startTime})</if> "
            , "<if test=\"endTime != null and endTime &gt; 0\">AND (created &lt;= #{endTime} OR updated &lt;= #{endTime})</if> "
            , "ORDER BY id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit}"
            , "</script>"})
    List<UserVerify> queryUserKycList(@Param("orgId") Long orgId,
                                      @Param("fromId") Long fromId,
                                      @Param("lastId") Long lastId,
                                      @Param("startTime") Long startTime,
                                      @Param("endTime") Long endTime,
                                      @Param("limit") Integer limit,
                                      @Param("orderDesc") Boolean orderDesc);

    @Select("select * from tb_user_verify where org_id=#{orgId} and verify_status = 2 order by id asc limit #{page},#{size}")
    List<UserVerify> getUserVerifyListByPage(@Param("orgId") Long orgId, @Param("page") int page, @Param("size") int size);
}
