package io.bhex.broker.server.statistics.clear.mapper;

import io.bhex.broker.server.model.StatisticsBalanceLockRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import java.util.List;

@Mapper
public interface StatisticsBalanceLockRecordMapper {

    @Select({"<script>"
            , "SELECT *"
            , "FROM ods_balance_lock_record a "
            , "WHERE a.org_id=#{orgId} "
            , "<if test=\"tokenId != null\">AND a.token_id = #{tokenId} </if> "
            , "<if test=\"accountId != null and accountId &gt; 0\">AND a.account_id = #{accountId}</if> "
            , "<if test=\"businessSubject != null and businessSubject &gt; 0\">AND a.business_subject = #{businessSubject}</if> "
            , "<if test=\"secondBusinessSubject != null and secondBusinessSubject &gt; 0\">AND a.second_business_subject = #{secondBusinessSubject}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND a.id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND a.id &gt; #{lastId}</if> "
            , "ORDER BY a.id <if test=\"orderDesc\">DESC</if>  "
            , "LIMIT #{limit} "
            , "</script>"})
    List<StatisticsBalanceLockRecord> queryBalanceLockRecordList(@Param("orgId") Long orgId,
                                                                 @Param("accountId") Long accountId,
                                                                 @Param("tokenId") String tokenId,
                                                                 @Param("businessSubject") Integer businessSubject,
                                                                 @Param("secondBusinessSubject") Integer secondBusinessSubject,
                                                                 @Param("status") Integer status,
                                                                 @Param("fromId") Long fromId,
                                                                 @Param("lastId") Long endId,
                                                                 @Param("limit") Integer limit,
                                                                 @Param("orderDesc") Boolean orderDesc);
}
