package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteRelation;
import io.bhex.broker.server.model.UserInviteInfo;
import org.apache.ibatis.annotations.*;

import java.util.Date;
import java.util.List;

@Mapper
public interface InviteRelationMapper extends tk.mybatis.mapper.common.Mapper<InviteRelation> {

//    @SelectProvider(type = InviteRelationSqlProvider.class, method = "getInviteRelationAccountId")
//    List<Long> getInviteRelationAccountId(@Param("start") int start, @Param("limit") int limit);

    @Select("select * from tb_invite_relation where org_id=#{orgId} and invited_id=#{invitedUserId}")
    List<InviteRelation> getInviteRelationsByInvitedId(@Param("orgId") Long orgId, @Param("invitedUserId") Long invitedUserId);

    @Select("select * from tb_invite_relation where org_id=#{orgId} and invited_id=#{invitedUserId} and invited_type = 1 limit 1")
    InviteRelation getDirectInviteRelationByInvitedId(@Param("orgId") Long orgId, @Param("invitedUserId") Long invitedUserId);

    @Select({"<script>"
            , "SELECT * FROM tb_invite_relation WHERE org_id=#{orgId} AND user_id=#{userId}"
            , "<if test=\"fromId != null and fromId &gt; 0\">AND id &gt; #{fromId}</if> "
            , "<if test=\"limit != null and limit &gt; 0\">ORDER BY id LIMIT #{limit}</if> "
            , "</script>"
    })
    List<InviteRelation> getInviteRelationByUserId(@Param("orgId") Long orgId,
                                                   @Param("userId") Long userId,
                                                   @Param("fromId") Long fromId,
                                                   @Param("limit") Integer limit);

    @Select({"<script>"
            , "SELECT a.id AS invite_id, b.user_id, b.national_code, b.mobile, b.email, b.register_type, "
            + " ifnull(c.verify_status, 0) as verify_status, c.first_name as first_name, c.second_name as second_name, "
            + "  c.data_secret as data_secret,c.data_encrypt as data_encrypt, c.nationality as nationality,"
            + "b.source, a.invited_type as invite_type, b.created, a.invited_hobbit_leader as invite_hobbit_leader "
            , "FROM tb_invite_relation a "
            + "JOIN tb_user b ON a.invited_id = b.user_id "
            + "LEFT JOIN tb_user_verify c ON a.invited_id=c.user_id "
            , "WHERE a.org_id=#{orgId} AND a.user_id=#{userId} "
            , "<if test=\"inviteType != null and inviteType &gt; 0\">AND a.invited_type = #{inviteType}</if> "
            , "<if test=\"invitedLeader\"> AND a.invited_hobbit_leader = 1 </if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.id &gt; #{lastId}</if> "
            , "<if test=\"startTime != null and startTime &gt; 0\">AND b.created &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null and endTime &gt; 0\">AND b.created &lt;= #{endTime}</if> "
            , "<if test=\"source != null and source != ''\">AND b.source = #{source}</if> "
            , "ORDER BY a.id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit} "
            , "</script>"
    })
    List<UserInviteInfo> queryUserInviteInfo(@Param("orgId") Long orgId,
                                             @Param("userId") Long userId,
                                             @Param("inviteType") Integer inviteType,
                                             @Param("invitedLeader") Boolean invitedLeader,
                                             @Param("fromId") Long fromId,
                                             @Param("lastId") Long lastId,
                                             @Param("startTime") Long startTime,
                                             @Param("endTime") Long endTime,
                                             @Param("source") String source,
                                             @Param("limit") Integer limit,
                                             @Param("orderDesc") Boolean orderDesc);

    @Update("update tb_invite_relation set invited_status = 0 where invited_id = #{userId}")
    void updateInviteRelationStatusByUserId(@Param("userId") Long userId);

    @Update("update tb_invite_relation set invited_hobbit_leader = #{invitedHobbitLeader} where org_id=#{orgId} AND invited_id = #{userId}")
    void updateInvitedHobbitLeaderByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("invitedHobbitLeader") int invitedHobbitLeader);

    @Select("select * from tb_invite_relation where org_id=#{orgId} and user_id=#{userId} and invited_type = 1")
    List<InviteRelation> getInviteRelationsByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select("select invited_id from tb_invite_relation where org_id=#{orgId} and user_id=#{userId} and invited_type = 1")
    List<Long> getDirectInvitedUsers(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select("select invited_id from tb_invite_relation where org_id=#{orgId} and user_id=#{userId} and invited_type = 1 and created_at > #{startTime}")
    List<Long> getDirectInvitedRelations(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("startTime") Date startTime);
}
