package io.bhex.broker.server.statistics.clear.mapper;

import io.bhex.broker.server.model.StatisticsWithdrawOrder;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.sql.Timestamp;
import java.util.List;

@Mapper
public interface StatisticsWithdrawOrderMapper {

    @Select({"<script>"
            , "SELECT "
            + "     a.withdrawal_order_id as order_id, a.client_withdrawal_id, a.broker_id as org_id, b.broker_user_id as user_id, a.account_id, a.token_id, "
            + "     a.address, a.address_tag, a.total_quantity, a.arrive_quantity, "
            + "     a.miner_fee, a.miner_fee_token_id, a.platform_fee, a.broker_fee, a.fee_token_id, "
            + "     a.tx_id, a.tx_id_url, a.status, a.created_at, a.updated_at, a.wallet_handle_time  "
            , "FROM ods_withdrawal_order a JOIN ods_account b ON a.account_id=b.account_id "
            , "WHERE a.broker_id=#{orgId} "
            , "<if test=\"userId != null and userId &gt; 0\">AND b.broker_user_id = #{userId}</if> "
            , "<if test=\"tokenId != null and tokenId != ''\">AND a.token_id = #{tokenId}</if> "
            , "<if test=\"address != null and address != ''\">AND a.address = #{address}</if> "
            , "<if test=\"txId != null and txId != ''\">AND a.tx_id = #{txId}</if> "
            , "<if test=\"startTime != null\">AND a.created_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND a.created_at &lt;= #{endTime}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.withdrawal_order_id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.withdrawal_order_id &gt; #{lastId}</if> "
            , "ORDER BY a.withdrawal_order_id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit}"
            , "</script>"})
    List<StatisticsWithdrawOrder> queryOrgWithdrawOrder(@Param("orgId") Long orgId,
                                                        @Param("userId") Long userId,
                                                        @Param("tokenId") String tokenId,
                                                        @Param("startTime") Timestamp startTime,
                                                        @Param("endTime") Timestamp endTime,
                                                        @Param("fromId") Long fromId,
                                                        @Param("lastId") Long lastId,
                                                        @Param("limit") Integer limit,
                                                        @Param("orderDesc") Boolean orderDesc,
                                                        @Param("address") String address,
                                                        @Param("txId") String txId);

    @Select("SELECT COUNT(1) FROM ods_withdrawal_order where broker_id=#{orgId} and broker_user_id=#{userId} AND status IN (1, 3, 5, 6)")
    int countByUserId(@Param("orgId") long orgId, @Param("userId") String userId);

    @Select("SELECT * FROM ods_withdrawal_order WHERE broker_id=#{orgId} and broker_user_id=#{userId} AND created_at >= #{startTime} AND created_at < #{endTime} AND `status` IN (1, 3, 5, 6)")
    List<StatisticsWithdrawOrder> getByUserIdWithGiveTime(@Param("orgId") long orgId, @Param("userId") Long userId, @Param("startTime") Timestamp startTime, @Param("endTime") Timestamp endTime);

}
