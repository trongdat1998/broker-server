package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.InviteInfo;
import org.apache.ibatis.jdbc.SQL;

import java.util.List;
import java.util.Map;

/**
 * @author duqingxiang
 * @date 2018-09-13
 */
public class InviteInfoSqlProvider {

    private static final String TABLE_NAME = "tb_invite_info";

    private static final String COLUMES = "id,org_id,user_id,account_id,invite_count,invite_vaild_count,invite_direct_vaild_count,"
            + "invite_indirect_vaild_count,invite_level,direct_rate,indirect_rate,bonus_coin,bonus_point,created_at,updated_at,invite_hobbit_leader_count,hide";


    public String updateById(InviteInfo inviteInfo) {
        return new SQL() {
            {
                UPDATE(TABLE_NAME);
                if (inviteInfo.getInviteCount() != null) {
                    SET("invite_count = #{inviteCount}");
                }
                if (inviteInfo.getInviteVaildCount() != null) {
                    SET("invite_vaild_count = #{inviteVaildCount}");
                }
                if (inviteInfo.getInviteLevel() != null) {
                    SET("invite_level = #{inviteLevel}");
                }

                if (inviteInfo.getDirectRate() != null) {
                    SET("direct_rate = #{directRate}");
                }
                if (inviteInfo.getIndirectRate() != null) {
                    SET("indirect_rate = #{indirectRate}");
                }

                if (inviteInfo.getBonusCoin() != null) {
                    SET("bonus_coin = #{bonusCoin}");
                }
                if (inviteInfo.getBonusPoint() != null) {
                    SET("bonus_point = #{bonusPoint}");
                }
                WHERE("user_id = #{userId}");
            }
        }.toString();
    }

    public String getInviteInfoByUserId() {
        return new SQL() {
            {
                SELECT(COLUMES).FROM(TABLE_NAME);
                WHERE(" user_id = #{userId}").ORDER_BY(" id desc ");
            }
        }.toString();
    }

    public String getInviteInfoListByUserId(Map<String, Object> parameter) {
        final List<Long> userIds = (List<Long>) parameter.get("userIds");
        StringBuilder sqlBuilder = new StringBuilder("select ").append(COLUMES).append(" from ").append(TABLE_NAME);
        sqlBuilder.append(" where org_id = #{orgId}");
        sqlBuilder.append(" and user_id in(");
        for (int i = 0; i < userIds.size(); i++) {
            sqlBuilder.append(String.format("#{userIds[%d]}", i));
            if (i < userIds.size() - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

}
