package io.bhex.broker.server.primary.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.jdbc.SQL;

import java.util.List;

import io.bhex.broker.server.model.InviteBonusRecord;
import lombok.Builder;
import lombok.Data;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface InviteBonusRecordMapper extends tk.mybatis.mapper.common.Mapper<InviteBonusRecord>, InsertListMapper<InviteBonusRecord> {

    String TABLE_NAME = "tb_invite_bonus_record";

    @Select("select * from tb_invite_bonus_record where org_id = #{orgId} and statistics_time = #{statisticsTime} "
            + " order by id asc limit #{startIndex},#{limit}")
    List<InviteBonusRecord> selectByOrgIdAndStatisticsTime(@Param("orgId") Long orgId,
                                                           @Param("statisticsTime") Long statisticsTime,
                                                           @Param("startIndex") int startIndex,
                                                           @Param("limit") int limit);

    @Select("select * from tb_invite_bonus_record where org_id = #{orgId} and statistics_time = #{statisticsTime} "
            + " order by id asc limit #{limit}")
    List<InviteBonusRecord> selectByOrgIdAndStatisticsTime2(@Param("orgId") Long orgId,
                                                            @Param("statisticsTime") Long statisticsTime,
                                                            @Param("limit") int limit);

    @Select("select * from tb_invite_bonus_record where "
            + " org_id = #{orgId} "
            + " and statistics_time = #{statisticsTime} "
            + " and status = #{status} "
            + " and id > #{lastId} "
            + " order by id asc limit #{startIndex},#{limit}")
    List<InviteBonusRecord> selectByOrgIdAndStatusAndStatisticsTime(@Param("orgId") Long orgId,
                                                                    @Param("status") Integer status,
                                                                    @Param("statisticsTime") Long statisticsTime,
                                                                    @Param("startIndex") int startIndex,
                                                                    @Param("limit") int limit,
                                                                    @Param("lastId") long lastId);

    @SelectProvider(type = InviteBonusRecordSQLProvider.class, method = "query")
    List<InviteBonusRecord> selectByCondition(QueryCondition condition);

    @Delete("delete from tb_invite_bonus_record where org_id=#{orgId} and statistics_time = #{statisticsTime}")
    int deleteInviteBonusRecordByTime(@Param("orgId") Long orgId, @Param("statisticsTime") Long statisticsTime);


    @Select("select count(1) from tb_invite_bonus_record where org_id = #{orgId} and statistics_time = #{statisticsTime}")
    int selectCountByOrgIdAndStatisticsTime(@Param("orgId") Long orgId,
                                       @Param("statisticsTime") Long statisticsTime);


    @Select("select count(1) from tb_invite_bonus_record where org_id = #{orgId} and statistics_time = #{statisticsTime} and status = 1")
    int selectCountSuccessByOrgIdAndStatisticsTime(@Param("orgId") Long orgId,
                                            @Param("statisticsTime") Long statisticsTime);

    @Select("select token,sum(bonus_amount) as bonus_amount  from tb_invite_bonus_record where org_id = #{orgId} and statistics_time = #{statisticsTime} group by token")
    List<InviteBonusRecord> selectBonusAmountByTime(@Param("orgId") Long orgId,
                                                            @Param("statisticsTime") Long statisticsTime);

    @Select("select token,sum(bonus_amount) as bonus_amount  from tb_invite_bonus_record " +
            "where org_id = #{orgId} and user_id = #{userId} and statistics_time = #{statisticsTime} group by token")
    List<InviteBonusRecord> selectUserBonusAmountByTime(@Param("orgId") Long orgId, @Param("userId") Long userId,
                                                    @Param("statisticsTime") Long statisticsTime);

    @Data
    @Builder
    class QueryCondition {

        private Long userId;

        private Integer status;

        private Integer nextId;

        private Integer beforeId;

        private Integer startIndex;

        private Integer limit;

        private String orderBy;

    }


    class InviteBonusRecordSQLProvider {

        public String query(final InviteBonusRecordMapper.QueryCondition condition) {


            String sql = new SQL() {
                {
                    SELECT("*").FROM(TABLE_NAME);

                    if (condition.getUserId() != null) {
                        WHERE("user_id = #{userId}");
                    }

                    if (condition.getStatus() != null) {
                        WHERE("status = #{status}");
                    }

                    if (condition.getNextId() != null && condition.getNextId() > 0) {
                        WHERE("id > #{nextId}");
                    }

                    if (condition.getBeforeId() != null && condition.getBeforeId() > 0) {
                        WHERE("id < #{beforeId}");
                    }

                    if (StringUtils.isNotBlank(condition.getOrderBy())) {
                        ORDER_BY(condition.getOrderBy());
                    }
                }
            }.toString();

            if (condition.getLimit() != null && condition.getLimit() > 0) {
                sql += (" limit " + condition.getStartIndex() + "," + condition.getLimit());
            }

            return sql;
        }

    }
}
