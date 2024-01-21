package io.bhex.broker.server.statistics.clear.mapper;

import io.bhex.broker.server.model.BalanceFlowSnapshot;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface StatisticsBalanceFlowMapper extends tk.mybatis.mapper.common.Mapper<BalanceFlowSnapshot> {

    @SelectProvider(type = StatisticsSqlProvider.class, method = "queryComeInBalanceFlow")
    List<BalanceFlowSnapshot> queryComeInBalanceFlow(@Param("tokenId") String tokenId,
                                                     @Param("accountId") List<Long> accountId,
                                                     @Param("startTime") String startTime,
                                                     @Param("endTime") String endTime,
                                                     @Param("businessSubject") Integer limit);

    @SelectProvider(type = StatisticsSqlProvider.class, method = "queryOutgoBalanceFlow")
    List<BalanceFlowSnapshot> queryOutgoBalanceFlow(@Param("tokenId") String tokenId,
                                                    @Param("accountId") List<Long> accountId,
                                                    @Param("startTime") String startTime,
                                                    @Param("endTime") String endTime,
                                                    @Param("businessSubject") Integer limit);

    @SelectProvider(type = StatisticsSqlProvider.class, method = "queryAccountBalanceFlow")
    List<BalanceFlowSnapshot> queryAccountBalanceFlow(@Param("accountId") Long accountId,
                                                      @Param("businessSubject") List<Integer> businessSubject,
                                                      @Param("lastId") Long lastId,
                                                      @Param("limit") Integer limit,
                                                      @Param("tokenId") String tokenId);

    @Select("select ifnull(sum(changed),0) from ods_balance_flow where account_id = #{accountId} and token_id = #{tokenId} and business_subject in(70,602)")
    BigDecimal sumAccountChanged(@Param("accountId") Long accountId,
                                 @Param("businessSubject") Integer businessSubject,
                                 @Param("tokenId") String tokenId);

}
