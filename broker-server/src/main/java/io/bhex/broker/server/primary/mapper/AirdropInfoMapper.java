package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AirdropInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.mapper
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 6:56 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface AirdropInfoMapper extends Mapper<AirdropInfo> {

    String TABLE_NAME = " tb_airdrop_info ";

    String COLUMNS = "id, title, description, type, broker_id, account_id, user_type, user_account_ids, airdrop_token_id, " +
            "airdrop_token_num, have_token_id, have_token_num, snapshot_time, user_count, transfer_asset_amount, is_schedule_jod, airdrop_time, " +
            "status, failed_reason, admin_id, updated_at, created_at";

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE broker_id = #{brokerId} order by created_at desc")
    List<AirdropInfo> listInfoByBrokerId(@Param("brokerId") Long brokerId);

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE id = #{id} and  broker_id = #{brokerId} for update")
    AirdropInfo getAirdropInfAndLock(@Param("id") Long id, @Param("brokerId") Long brokerId);

    @Update("UPDATE " + TABLE_NAME + " SET status=#{status} WHERE id = #{id} AND broker_id = #{brokerId} AND status=#{oldStatus};")
    int updateStatus(@Param("id") Long id,
                     @Param("brokerId") Long brokerId,
                     @Param("status") Integer status,
                     @Param("oldStatus") Integer oldStatus);
}
