package io.bhex.broker.server.statistics.statistics.mapper;

import io.bhex.broker.server.model.StatisticsBalanceSnapshot;
import io.bhex.broker.server.model.staking.StakingBalanceSnapshot;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

@Mapper
public interface StatisticsBalanceSnapshotMapper {

    @Select("SELECT b.org_id, a.dt as statistics_time, a.broker_user_id as user_id, a.account_id, a.token_id, a.total, a.updated_at as activity_time "
            + "FROM snap_balance a "
            + "WHERE a.dt=#{dt} AND a.token_id=#{tokenId} AND a.org_id=#{orgId} AND a.total > 0")
    List<StatisticsBalanceSnapshot> queryTokenBalanceSnapshotByDate(@Param("orgId") Long orgId, @Param("dt") String statisticsTime, @Param("tokenId") String tokenId);

    @Select("SELECT b.id, b.org_id, a.dt as statistics_time, a.broker_user_id as user_id, a.account_id, a.token_id, a.total, a.updated_at as activity_time "
            + "FROM snap_balance a "
            + "WHERE a.dt=#{dt} AND a.token_id=#{tokenId} AND a.org_id=#{orgId} AND a.total >= #{total} and b.id > #{id} order by b.id asc limit #{limit} ")
    List<StakingBalanceSnapshot> listTokenBalanceSnapshot(@Param("orgId") Long orgId
            , @Param("dt") String statisticsTime
            , @Param("tokenId") String tokenId
            , @Param("total") BigDecimal total
            , @Param("limit") Integer limit
            , @Param("id") Long id);

    /**
     * 理财矿池用，功能暂停
     * @param statisticsTime
     * @param tokenId
     * @param total
     * @return
     */
    @Deprecated
    @Select("SELECT sum(a.total)"
            + "FROM snap_balance a"
            + "WHERE a.dt=#{dt} AND a.token_id=#{tokenId} AND a.total >= #{total}")
    BigDecimal getPlatformHoldTotal(@Param("dt") String statisticsTime, @Param("tokenId") String tokenId, @Param("total") BigDecimal total);

    /**
     *
     * @param accountId
     * @param tokenId
     * @param startDate
     * @param endDate
     * @return
     */
    @Select("SELECT ifnull(sum(total),0) FROM rpt_asset_by_flow WHERE account_id = #{accountId} and token_id = #{tokenId} and dt between #{startDate} and #{endDate}")
    BigDecimal getTokenBalanceSnapshotTotalByAccountId(@Param("accountId") Long accountId, @Param("tokenId") String tokenId, @Param("startDate") String startDate
            , @Param("endDate") String endDate);

    @Select(" <script> "
            + " SELECT ifnull(sum(total),0) FROM snap_balance WHERE account_id = #{accountId} and token_id = #{tokenId} and dt in "
            + " <foreach item='item' index='index' collection='times' open='(' separator=',' close=')'> "
            + " #{item} "
            + " </foreach> "
            + " </script> ")
    BigDecimal queryTokenBalanceSnapshotCountByAccountId(@Param("accountId") Long accountId, @Param("tokenId") String tokenId, @Param("times") Set<String> times);

    @Select(" <script> "
            +"select * from snap_balance where dt = #{dt} and account_id in "
            + " <foreach item='item' index='index' collection='accountIds' open='(' separator=',' close=')'> "
            + " #{item}"
            + " </foreach>"
            + " </script>")
    List<StakingBalanceSnapshot> queryBalanceSnapshotByAccountIds(@Param("dt") String dt,@Param("accountIds") Set<Long> accountIds);

    @Select(" <script> "
            + " SELECT org_id, dt as statistics_time, broker_user_id as user_id, account_id, token_id, total, updated_at as activity_time FROM snap_balance WHERE dt = #{dt} and token_id = #{tokenId} and account_id in "
            + " <foreach item='item' index='index' collection='accountIds' open='(' separator=',' close=')'> "
            + " #{item} "
            + " </foreach> "
            + " </script> ")
    List<StatisticsBalanceSnapshot> queryAssetSnapByAccountIds(@Param("accountIds")List<Long> accountIds, @Param("tokenId") String tokenId, @Param("dt")String dt);
}
