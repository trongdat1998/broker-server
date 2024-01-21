package io.bhex.broker.server.statistics.statistics.mapper;

import io.bhex.broker.server.model.ContractSnapshot;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * 暂时屏蔽合约大赛入口
 */
@Mapper
public interface StatisticsContractSnapshotMapper extends tk.mybatis.mapper.common.Mapper<ContractSnapshot> {

    @Select("select max(snapshot_time) from rpt_contract_snapshot where contract_id = #{contractId}")
    String queryMaxTime(@Param("contractId") String contractId);

    @Select("select max(snapshot_time) from rpt_contract_snapshot")
    String queryAllMaxTime();

    @Select("select max(snapshot_time) from rpt_contract_snapshot where contract_id = #{contractId} and created_at <= #{endTime}")
    String queryMaxTimeByTime(@Param("contractId") String contractId, @Param("endTime") String endTime);

    @Select("select snapshot_time from rpt_contract_snapshot where created_at < #{startTime} and contract_id = #{contractId} order by snapshot_time desc limit 0,1")
    String queryEarliestTimeByTime(@Param("startTime") String startTime, @Param("contractId") String contractId);

    @Select("select snapshot_time from rpt_contract_snapshot where created_at < #{startTime} and contract_id = #{contractId} order by snapshot_time desc limit 0,1")
    String queryDailySnapshot(@Param("startTime") String startTime, @Param("contractId") String contractId);

    @Select("select * from rpt_contract_snapshot where account_id=#{accountId} and contract_id = #{contractId} " +
            "and created_at >= #{startTime} and contract_balance>=#{limit} order by created_at limit 2")
    List<ContractSnapshot> listSnapshot(@Param("contractId") String contractId, @Param("startTime") String startTime,
                                        @Param("accountId") Long accountId, @Param("limit") BigDecimal limit);

    @Select("select snapshot_time from rpt_contract_snapshot where contract_id in (${contractId}) and account_id=#{accountId} " +
            "and created_at >= #{startTime} order by created_at asc limit 1")
    String querySnapshotTime(@Param("contractId") String contractId, @Param("startTime") String startTime,
                             @Param("accountId") Long accountId);
}
