package io.bhex.broker.server.statistics.clear.mapper;

import io.bhex.broker.server.model.StatisticsDepositOrder;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.sql.Timestamp;
import java.util.List;

@Mapper
public interface StatisticsDepositOrderMapper {

    @Select({"<script>"
            , "SELECT "
            + "     a.deposit_order_id as order_id, a.org_id, b.broker_user_id as user_id, a.account_id, a.token_id, a.quantity, a.from_address, "
            + "     a.wallet_address, a.wallet_address_tag, a.tx_id, a.tx_id_url, a.status, a.created_at, a.updated_at, a.deposit_receipt_type, a.cannot_receipt_reason "
            , "FROM ods_deposit_order a JOIN ods_account b ON a.account_id=b.account_id "
            , "WHERE a.org_id=#{orgId} "
            , "<if test=\"userId != null and userId &gt; 0\">AND b.broker_user_id = #{userId}</if> "
            , "<if test=\"tokenId != null and tokenId != ''\">AND a.token_id = #{tokenId}</if> "
            , "<if test=\"address != null and address != ''\">AND a.wallet_address = #{address}</if> "
            , "<if test=\"txId != null and txId != ''\">AND a.tx_id = #{txId}</if> "
            , "<if test=\"startTime != null\">AND a.created_at &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND a.created_at &lt;= #{endTime}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.deposit_order_id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.deposit_order_id &gt; #{lastId}</if> "
            , "ORDER BY a.deposit_order_id <if test=\"orderDesc\">DESC</if> "
            , "LIMIT #{limit} "
            , "</script>"})
    List<StatisticsDepositOrder> queryOrgDepositOrder(@Param("orgId") Long orgId,
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
}
