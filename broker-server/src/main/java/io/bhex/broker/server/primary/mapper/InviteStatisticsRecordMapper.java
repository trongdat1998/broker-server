package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteStatisticsRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.special.InsertListMapper;

@Mapper
public interface InviteStatisticsRecordMapper extends tk.mybatis.mapper.common.Mapper<InviteStatisticsRecord>, InsertListMapper<InviteStatisticsRecord> {

    @Update("update tb_invite_statistics_record set transfer_amount = transfer_amount  + #{transferAmount}, "
            + " status = IF(transfer_amount  + #{transferAmount} >= amount, 1, status)"
            + " where org_id=#{orgId} and invite_type=#{inviteType} and statistics_time = #{statisticsTime} and token = #{token} ")
    int incrTransferAmount(@Param("orgId") long orgId, @Param("inviteType") Integer inviteType,
                           @Param("statisticsTime") Long statisticsTime, @Param("token") String token,
                           @Param("transferAmount") Double transferAmount);

}
