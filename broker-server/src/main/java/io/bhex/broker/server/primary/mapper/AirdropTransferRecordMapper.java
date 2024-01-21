package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AirdropTransferRecord;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.mapper
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 7:23 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface AirdropTransferRecordMapper extends Mapper<AirdropTransferRecord>, InsertListMapper<AirdropTransferRecord> {


    @Update("update tb_airdrop_transfer_record set status = #{status} "
            + " where broker_id = #{brokerId} and airdrop_id = #{airdropId} and group_id = #{groupId}")
    int updateStatus(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId,
                     @Param("groupId") long groupId, @Param("status") int status);

    @Update("update tb_airdrop_transfer_record set locked_status = #{lockedStatus} "
            + " where broker_id = #{brokerId} and airdrop_id = #{airdropId} and group_id = #{groupId}")
    int updateLockedStatus(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId,
                           @Param("groupId") long groupId, @Param("lockedStatus") int lockedStatus);

    @Update("update tb_airdrop_transfer_record set msg_noticed = 1 "
            + " where broker_id = #{brokerId} and airdrop_id = #{airdropId}")
    int updateMsgNoticedStatus(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId);

    @Update("update tb_airdrop_transfer_record set msg_noticed = 1 where broker_id = #{brokerId}")
    int updateBrokerMsgNoticedStatus(@Param("brokerId") long brokerId);

    @Update({"<script>",
            "update tb_airdrop_transfer_record set status=#{status} "
                    + " where broker_id = #{brokerId} and airdrop_id = #{airdropId} and group_id = #{groupId} and id in "
                    + " <foreach collection=\"recordIds\" index=\"index\" item=\"item\" open=\"(\" separator=\",\" close=\")\">"
                    + "#{item}"
                    + "</foreach>"
                    + "</script>"})
    int updateStatusByGroup(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId, @Param("groupId") long groupId,
                            @Param("recordIds") List<Long> recordIds, @Param("status") Integer status);

}
