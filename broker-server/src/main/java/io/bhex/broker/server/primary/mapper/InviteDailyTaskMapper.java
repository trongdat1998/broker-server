package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteDailyTask;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface InviteDailyTaskMapper extends tk.mybatis.mapper.common.Mapper<InviteDailyTask> {

    @Update("update tb_invite_daily_task set change_status = 1 where org_id = #{orgId} and statistics_time = #{statisticsTime}")
    int updateInviteDailyTaskChangeStatusToFail(@Param("orgId") Long orgId, @Param("statisticsTime") Long statisticsTime);

    @Update("update tb_invite_daily_task set change_status = 0 where org_id = #{orgId} and statistics_time = #{statisticsTime}")
    int updateInviteDailyTaskChangeStatusToSuccess(@Param("orgId") Long orgId, @Param("statisticsTime") Long statisticsTime);

    @Update("update tb_invite_daily_task set change_status = 2 where org_id = #{orgId} and statistics_time = #{statisticsTime}")
    int updateInviteDailyTaskChangeStatusToProcessing(@Param("orgId") Long orgId, @Param("statisticsTime") Long statisticsTime);
}
