package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteInfo;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface InviteInfoMapper extends tk.mybatis.mapper.common.Mapper<InviteInfo> {


    @SelectProvider(type = InviteInfoSqlProvider.class, method = "getInviteInfoByUserId")
    InviteInfo getInviteInfoByUserId(@Param("userId") long userId);

    @SelectProvider(type = InviteInfoSqlProvider.class, method = "getInviteInfoListByUserId")
    List<InviteInfo> getInviteInfoListByUserId(@Param("orgId") Long orgId, @Param("userIds") List<Long> userIds);

    @UpdateProvider(type = InviteInfoSqlProvider.class, method = "updateById")
    int updateById(InviteInfo inviteInfo);

    @Update("update tb_invite_info set invite_direct_vaild_count = invite_direct_vaild_count + #{directValidCount} ,invite_indirect_vaild_count = invite_indirect_vaild_count + #{indirectValidCount} where id = #{id}")
    int updateValidCount(@Param("id") Long id, @Param("directValidCount") int directValidCount, @Param("indirectValidCount") int indirectValidCount);

    @Update("update tb_invite_info set invite_hobbit_leader_count = #{number} where org_id=#{orgId} AND user_id = #{userId}")
    void updateInvitedHobbitLeaderByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("number") int number);
}
