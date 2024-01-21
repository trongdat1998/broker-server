/*
 ************************************
 * @项目名称: broker
 * @文件名称: UserMapper
 * @Date 2018/05/22
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.SimpleUserInfo;
import io.bhex.broker.server.model.User;
import org.apache.ibatis.annotations.*;

import java.util.List;


@Mapper
public interface UserMapper extends tk.mybatis.mapper.common.Mapper<User> {

    @InsertProvider(type = UserSqlProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insertRecord(User user);

    /**
     * 使用这个方法的时候注意可能有坑。里面用的判断条件是Strings.isNullOrEmpty
     */
    @UpdateProvider(type = UserSqlProvider.class, method = "update")
    int updateRecord(User user);

    @Update("update tb_user set user_status = #{newStatus} where user_id=#{userId} and user_status = #{oldStatus}")
    Integer updateUserStatus(@Param("userId") Long userId, @Param("newStatus") int newStatus, @Param("oldStatus") int oldStatus);

    @Update("update tb_user set national_code = '', mobile = '', concat_mobile = '', register_type=2 where org_id=#{orgId} and user_id=#{userId} and email <> '' ")
    Integer unbindUserMobile(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Update("update tb_user set email = '', email_alias = '', register_type=1 where org_id=#{orgId} and user_id=#{userId} and mobile <> '' ")
    Integer unbindUserEmail(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Update("update tb_user set bind_ga = 0 , second_level_invite_user_id = 0 where org_id=#{orgId} and user_id=#{userId} ")
    Integer unbindGA(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Update("update tb_user set invite_user_id = 0,second_level_invite_user_id = 0 where org_id=#{orgId} and user_id=#{userId} ")
    Integer cancelInviteRelation(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Update("update tb_user set second_level_invite_user_id = 0 where org_id=#{orgId} and user_id=#{userId} ")
    Integer cancelSecondLevelInviteRelation(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @SelectProvider(type = UserSqlProvider.class, method = "getByUserId")
    User getByUserId(Long userId);

    @SelectProvider(type = UserSqlProvider.class, method = "getByOrgAndUserId")
    User getByOrgAndUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select({"<script>"
            , "SELECT * FROM tb_user WHERE org_id=#{orgId}"
            , "<if test=\"userId != null and userId &gt; 0\">AND user_id = #{userId} </if>"
            , "<if test=\"mobile != null and mobile != ''\">AND mobile = #{mobile} </if>"
            , "<if test=\"email != null and email != ''\">AND (email = #{email} OR email_alias = #{email})</if>"
            , "</script>"
    })
    User findUser(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("mobile") String mobile, @Param("email") String email);

    @Select("SELECT count(*) FROM tb_user WHERE org_id=#{orgId} AND (email=#{email} OR email_alias=#{emailAlias})")
    int countEmailExists(@Param("orgId") Long orgId, @Param("email") String email, @Param("emailAlias") String emailAlias);

    @SelectProvider(type = UserSqlProvider.class, method = "getByEmail")
    User getByEmail(@Param("orgId") Long orgId, @Param("email") String email);

    @SelectProvider(type = UserSqlProvider.class, method = "getByEmailAlias")
    User getByEmailAlias(@Param("orgId") Long orgId, @Param("emailAlias") String emailAlias);

    @SelectProvider(type = UserSqlProvider.class, method = "getByMobile")
    User getByMobile(@Param("orgId") Long orgId, @Param("nationalCode") String nationalCode, @Param("mobile") String mobile);

    @Select("select * from tb_user where org_id = #{orgId} and (mobile = #{mobile} or concat_mobile = #{mobile}) limit 0,1")
    User getByMobileOrConcatMobile(@Param("orgId") Long orgId, @Param("mobile") String mobile);

    @Select("select * from tb_user where org_id = #{orgId} and concat_mobile = #{mobile} limit 0,1")
    User getByConcatMobile(@Param("orgId") Long orgId, @Param("mobile") String mobile);

    @SelectProvider(type = UserSqlProvider.class, method = "getByUsername")
    User getByUsername(@Param("orgId") Long orgId, @Param("username") String username);

    @Select("select *from tb_user where org_id = #{orgId} and mobile = #{mobile} limit 0,1")
    User getByMobileAndOrgId(@Param("orgId") Long orgId, @Param("mobile") String mobile);

    @SelectProvider(type = UserSqlProvider.class, method = "getByInviteCode")
    User getByInviteCode(@Param("orgId") Long orgId, @Param("inviteCode") String inviteCode);

    @Select("<script>select u.* from tb_user u left join tb_otc_white_list w on u.org_id=w.org_id "
            + "and u.user_id=w.user_id where w.org_id = #{orgId} "
            + " <if test=\"userId != null\">and w.user_id = #{userId} </if> "
            + " limit #{offset}, #{size}</script>")
    List<User> getOtcWhiteListUser(@Param("orgId") Long orgId,
                                   @Param("userId") Long userId,
                                   @Param("offset") Integer offset,
                                   @Param("size") Integer size);

    @Select("select * from tb_user where id > #{lastId} and user_status = 1 order by id asc limit #{limit}")
    List<User> getUsers(@Param("lastId") Long lastId, @Param("limit") Integer limit);

    @Select("select org_id from tb_user where user_id = #{userId} limit 0,1")
    Long getOrgIdByUserId(@Param("userId") Long userId);

    @Select("select user_id,national_code,org_id from tb_user where national_code is not null")
    List<User> getNationalCodeList();

    @SelectProvider(type = UserSqlProvider.class, method = "listUpdateUserByDate")
    List<User> listUpdateUserByDate(@Param("orgId") long orgId,
                                    @Param("fromId") long fromId,
                                    @Param("endId") long endId,
                                    @Param("startTime") long startTime,
                                    @Param("endTime") long endTime,
                                    @Param("limit") int limit);

    @Select({"<script>"
            , "SELECT a.user_id, a.national_code, a.mobile, a.email, a.register_type, a.user_type, 0 as verify_status, "
            + "a.invite_user_id, a.second_level_invite_user_id, a.source, a.created,a.user_status "
            , "FROM tb_user a"
            , "WHERE a.org_id=#{orgId} "
            , "<if test=\"source != null and source != ''\">AND a.source = #{source}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.user_id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.user_id &gt; #{lastId}</if> "
            , "<if test=\"startTime != null and startTime &gt; 0\">AND a.created &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null and endTime &gt; 0\">AND a.created &lt;= #{endTime}</if> "
            , "ORDER BY a.user_id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit}"
            , "</script>"
    })
    List<SimpleUserInfo> queryUserInfo(@Param("orgId") Long orgId,
                                       @Param("source") String source,
                                       @Param("fromId") Long fromId,
                                       @Param("lastId") Long lastId,
                                       @Param("startTime") Long startTime,
                                       @Param("endTime") Long endTime,
                                       @Param("limit") Integer limit,
                                       @Param("orderDesc") Boolean orderDesc);

/*    @Select({"<script>"
            , "SELECT a.user_id, a.national_code, a.mobile, a.email, a.register_type, a.user_type, ifnull(b.verify_status, 0) as verify_status, "
            + "a.invite_user_id, a.second_level_invite_user_id, a.source, a.created,a.user_status "
            , "FROM tb_user a LEFT JOIN tb_user_verify b ON a.user_id=b.user_id "
            , "WHERE a.org_id=#{orgId} "
            , "<if test=\"source != null and source != ''\">AND a.source = #{source}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.user_id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.user_id &gt; #{lastId}</if> "
            , "<if test=\"startTime != null and startTime &gt; 0\">AND (a.created &gt;= #{startTime} OR a.updated &gt;= #{startTime})</if> "
            , "<if test=\"endTime != null and endTime &gt; 0\">AND ( a.created &lt;= #{endTime} OR a.updated &lt;= #{endTime})</if> "
            , "ORDER BY a.user_id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit}"
            , "</script>"
    })
    List<SimpleUserInfo> queryChangedUserInfo(@Param("orgId") Long orgId,
                                              @Param("source") String source,
                                              @Param("fromId") Long fromId,
                                              @Param("lastId") Long lastId,
                                              @Param("startTime") Long startTime,
                                              @Param("endTime") Long endTime,
                                              @Param("limit") Integer limit,
                                              @Param("orderDesc") Boolean orderDesc);

    @Select("select * from tb_user where org_id=#{orgId} order by id asc limit #{page},#{size}")
    List<User> getUserList(@Param("orgId") Long orgId, @Param("page") int page, @Param("size") int size);*/


    @Select({"<script>"
            , "SELECT e.* from ( "
            , " SELECT a.user_id, a.national_code, a.mobile, a.email, a.register_type, a.user_type, ifnull(b.verify_status, 0) as verify_status, "
            + " a.invite_user_id, a.second_level_invite_user_id, a.source, a.created,a.user_status "
            , " FROM tb_user a LEFT JOIN tb_user_verify b ON a.user_id=b.user_id "
            , " WHERE a.org_id=#{orgId} "
            , " <if test=\"source != null and source != ''\"> AND a.source = #{source}</if> "
            , " <if test=\"fromId != null and fromId &gt; 0\"> AND a.user_id &lt; #{fromId}</if> "
            , " <if test=\"lastId != null and lastId &gt; 0\"> AND a.user_id &gt; #{lastId}</if> "
            , " <if test=\"startTime != null and startTime &gt; 0\"> AND a.created &gt;= #{startTime}</if> "
            , " <if test=\"endTime != null and endTime &gt; 0\"> AND a.created &lt;= #{endTime}</if> "

            , " UNION "

            , " SELECT c.user_id, c.national_code, c.mobile, c.email, c.register_type, c.user_type, ifnull(d.verify_status, 0) as verify_status, "
            + " c.invite_user_id, c.second_level_invite_user_id, c.source, c.created,c.user_status "
            , " FROM tb_user c LEFT JOIN tb_user_verify d ON c.user_id=d.user_id "
            , " WHERE c.org_id=#{orgId} "
            , " <if test=\"source != null and source != ''\"> AND c.source = #{source}</if> "
            , " <if test=\"fromId != null and fromId &gt; 0\"> AND c.user_id &lt; #{fromId}</if> "
            , " <if test=\"lastId != null and lastId &gt; 0\"> AND c.user_id &gt; #{lastId}</if> "
            , " <if test=\"startTime != null and startTime &gt; 0\"> AND c.updated &gt;= #{startTime}</if> "
            , " <if test=\"endTime != null and endTime &gt; 0\"> AND c.updated &lt;= #{endTime}</if> "

            , " ) as e "
            , " ORDER BY e.user_id <if test=\"orderDesc\">DESC</if> "
            , " LIMIT #{limit}"
            , "</script>"
    })
    List<SimpleUserInfo> queryChangedUserInfo(@Param("orgId") Long orgId,
                                              @Param("source") String source,
                                              @Param("fromId") Long fromId,
                                              @Param("lastId") Long lastId,
                                              @Param("startTime") Long startTime,
                                              @Param("endTime") Long endTime,
                                              @Param("limit") Integer limit,
                                              @Param("orderDesc") Boolean orderDesc);

    @Select("select * from tb_user where org_id=#{orgId} order by id asc limit #{page},#{size}")
    List<User> getUserList(@Param("orgId") Long orgId, @Param("page") int page, @Param("size") int size);
}


