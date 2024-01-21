package io.bhex.broker.server.statistics.statistics.mapper;


import com.google.api.client.util.Lists;
import io.bhex.broker.server.domain.StatisticsTradeAmountData;
import io.bhex.broker.server.domain.StatisticsUserLevelData;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Mapper
public interface StatisticsUserLevelDataMapper {

    @SelectProvider(type = SqlProvider.class, method = "listBalanceAmount")
    List<StatisticsUserLevelData> listBalanceAmountByUsers(@Param("brokerId") long brokerId, @Param("tokenId") String tokenId, @Param("dayNum") int dayNum,
                                                           @Param("brokerUserIds") List<Long> brokerUserIds,
                                                           @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount);

    @SelectProvider(type = SqlProvider.class, method = "listBalanceAmount")
    List<StatisticsUserLevelData> listBalanceAmount(@Param("brokerId") long brokerId, @Param("tokenId") String tokenId, @Param("dayNum") int dayNum,
                                                    @Param("lastUserId") Long lastUserId, @Param("limit") int limit,
                                                    @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount);

    @SelectProvider(type = SqlProvider.class, method = "listSpotUserFees")
    List<StatisticsUserLevelData> listSpotUserFeeByUsers(@Param("brokerId") long brokerId, @Param("accountIds") List<Long> accountIds,
                                      @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount);

    @SelectProvider(type = SqlProvider.class, method = "listSpotUserFees")
    List<StatisticsUserLevelData> listSpotUserFees(@Param("brokerId") long brokerId, @Param("lastAccountId") Long lastAccountId, @Param("limit") int limit,
                                                   @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount);

    @SelectProvider(type = SqlProvider.class, method = "listContractUserFees")
    List<StatisticsUserLevelData> listContractUserFeeByUsers(@Param("brokerId") long brokerId, @Param("accountIds") List<Long> accountIds,
                                                         @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount);

    @SelectProvider(type = SqlProvider.class, method = "listContractUserFees")
    List<StatisticsUserLevelData> listContractUserFees(@Param("brokerId") long brokerId, @Param("lastAccountId") Long lastAccountId, @Param("limit") int limit,
                                                   @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount);


    @SelectProvider(type = SqlProvider.class, method = "listTradeAmountBtc")
    List<StatisticsUserLevelData> listTradeAmountBtcByUsers(@Param("brokerId") long brokerId, @Param("tradeType") int tradeType, @Param("userIds") List<Long> userIds,
                                                            @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount,
                                       @Param("startDate") String startDate, @Param("endDate") String endDate);

    @SelectProvider(type = SqlProvider.class, method = "listTradeAmountBtc")
    List<StatisticsUserLevelData> listTradeAmountBtc(@Param("brokerId") long brokerId, @Param("tradeType") int tradeType, @Param("lastUserId") Long lastUserId,
                                                  @Param("limit") int limit, @Param("minAmount") BigDecimal minAmount, @Param("maxAmount") BigDecimal maxAmount,
                                                            @Param("startDate") String startDate, @Param("endDate") String endDate);

    @Select("select trade_type,sum(buy_amount_btc + sell_amount_btc) amount from rpt_account_trade_summary_day " +
            " where dt between #{startDate} and #{endDate}  and broker_id = #{brokerId} " +
            " and broker_user_id = #{userId} " +
            " group by trade_type")
    List<StatisticsTradeAmountData> tradeAmountBtcByUser(@Param("brokerId") long brokerId, @Param("userId") String userId,
                                                         @Param("startDate") String startDate, @Param("endDate") String endDate);

    @Select("select trade_type,sum(buy_fee_total_usdt + sell_fee_total_usdt) amount from rpt_account_trade_summary_day " +
            " where broker_id = #{brokerId} and broker_user_id = #{userId} " +
            " group by trade_type")
    List<StatisticsTradeAmountData> tradeFeeUsdtByUser(@Param("brokerId") long brokerId, @Param("userId") String userId);

    @Slf4j
    class SqlProvider {

        public String listBalanceAmount(Map<String, Object> parameter) {
            StringBuilder sqlBuilder = new StringBuilder("select ");
            final Integer dayNum = (Integer) parameter.getOrDefault("dayNum", 1);

            if (dayNum == 1) {
                sqlBuilder.append(" sum_total_1day as value,");
            } else if (dayNum == 7) {
                sqlBuilder.append(" sum_total_7day as value,");
            } else if (dayNum == 30) {
                sqlBuilder.append(" sum_total_30day as value,");
            }

            sqlBuilder.append(" broker_user_id as user_id, broker_user_id as id from rpt_balance_snap_avg ");
            sqlBuilder.append(" where org_id = #{brokerId} and token_id=#{tokenId} ");

            final List<Long> brokerUserIds = (List<Long>) parameter.getOrDefault("brokerUserIds", Lists.newArrayList());
            if (!CollectionUtils.isEmpty(brokerUserIds)) {
                sqlBuilder.append(" and broker_user_id in(");
                sqlBuilder.append(String.join(",", brokerUserIds.stream().map(s -> s + "").collect(Collectors.toList())));
                sqlBuilder.append(")");
            }

            final Long lastUserId = (Long) parameter.getOrDefault("lastUserId", 0L);
            sqlBuilder.append(" and broker_user_id > ").append(lastUserId).append(" ");


            sqlBuilder.append(" group by broker_user_id having(value >= #{minAmount} ");
            final BigDecimal maxAmount = (BigDecimal) parameter.getOrDefault("maxAmount", BigDecimal.ZERO);
            if (maxAmount.compareTo(BigDecimal.ZERO) > 0) {
                sqlBuilder.append(" and value < #{maxAmount} ");
            }
            sqlBuilder.append(") ");

            sqlBuilder.append(" order by broker_user_id asc ");


            final Integer limit = (Integer) parameter.getOrDefault("limit", 0);
            if (limit > 0) {
                sqlBuilder.append(" limit ").append(limit);
            }
            log.info("sql:{}",sqlBuilder.toString());
            return sqlBuilder.toString();
        }

        public String listSpotUserFees(Map<String, Object> parameter) {
            StringBuilder sqlBuilder = new StringBuilder("select sum(total_fee_usd) value, account_id spot_account_id, account_id as id from rpt_user_fee_spot_trading_day ");
            sqlBuilder.append(" where broker_id = #{brokerId} ");

            final List<Long> accountIds = (List<Long>) parameter.getOrDefault("accountIds", Lists.newArrayList());
            if (!CollectionUtils.isEmpty(accountIds)) {
                sqlBuilder.append(" and account_id in(");
                for (int i = 0; i < accountIds.size(); i++) {
                    sqlBuilder.append(String.format("#{accountIds[%d]}", i));
                    if (i < accountIds.size() - 1) {
                        sqlBuilder.append(",");
                    }
                }
                sqlBuilder.append(")");
            }

            final Long lastAccountId = (Long) parameter.getOrDefault("lastAccountId", 0L);
            sqlBuilder.append(" and account_id > ").append(lastAccountId);


            sqlBuilder.append(" group by account_id having(value >= #{minAmount} ");
            final BigDecimal maxAmount = (BigDecimal) parameter.getOrDefault("maxAmount", BigDecimal.ZERO);
            if (maxAmount.compareTo(BigDecimal.ZERO) > 0) {
                sqlBuilder.append(" and value < #{maxAmount} ");
            }
            sqlBuilder.append(") ");

            sqlBuilder.append(" order by account_id asc ");


            final Integer limit = (Integer) parameter.getOrDefault("limit", 0);
            if (limit > 0) {
                sqlBuilder.append(" limit ").append(limit);
            }
            log.info("sql:{}",sqlBuilder.toString());
            return sqlBuilder.toString();
        }

        public String listContractUserFees(Map<String, Object> parameter) {
            StringBuilder sqlBuilder = new StringBuilder("select sum(total_fee_usd) value, account_id as contract_account_id, account_id as id from rpt_user_fee_contract_trading_day ");
            sqlBuilder.append(" where broker_id = #{brokerId} ");

            final List<Long> accountIds = (List<Long>) parameter.getOrDefault("accountIds", Lists.newArrayList());
            if (!CollectionUtils.isEmpty(accountIds)) {
                sqlBuilder.append(" and account_id in(");
                for (int i = 0; i < accountIds.size(); i++) {
                    sqlBuilder.append(String.format("#{accountIds[%d]}", i));
                    if (i < accountIds.size() - 1) {
                        sqlBuilder.append(",");
                    }
                }
                sqlBuilder.append(")");
            }

            final Long lastAccountId = (Long) parameter.getOrDefault("lastAccountId", 0L);
            sqlBuilder.append(" and account_id > ").append(lastAccountId);


            sqlBuilder.append(" group by account_id having(value >= #{minAmount} ");
            final BigDecimal maxAmount = (BigDecimal) parameter.getOrDefault("maxAmount", BigDecimal.ZERO);
            if (maxAmount.compareTo(BigDecimal.ZERO) > 0) {
                sqlBuilder.append(" and value < #{maxAmount} ");
            }
            sqlBuilder.append(") ");

            sqlBuilder.append(" order by account_id asc ");


            final Integer limit = (Integer) parameter.getOrDefault("limit", 0);
            if (limit > 0) {
                sqlBuilder.append(" limit ").append(limit);
            }
            log.info("sql:{}",sqlBuilder.toString());
            return sqlBuilder.toString();
        }


        public String listTradeAmountBtc(Map<String, Object> parameter) {
//            final Integer tradeType = (Integer) parameter.getOrDefault("tradeType", Lists.newArrayList());
            StringBuilder sqlBuilder = new StringBuilder("select sum(buy_amount_btc + sell_amount_btc) value, broker_user_id as id , broker_user_id as user_id  ");
//            if (tradeType == 1) {
//                sqlBuilder.append(" ,account_id as spot_account_id ");
//            } else if (tradeType == 2) {
//                sqlBuilder.append(" ,account_id as option_account_id ");
//            } else if (tradeType == 3) {
//                sqlBuilder.append(" ,account_id as contract_account_id ");
//            }
            sqlBuilder.append(" from rpt_account_trade_summary_day ");
            sqlBuilder.append(" where dt between #{startDate} and #{endDate}  and broker_id = #{brokerId} and trade_type = #{tradeType} ");
           // sqlBuilder.append(" and buy_fee_token not in('TBTC', 'TUSDT', 'TETH', 'BUSDT', 'CBTC')  and sell_fee_token not in('TBTC', 'TUSDT', 'TETH', 'BUSDT', 'CBTC')");

            final List<Long> userIds = (List<Long>) parameter.getOrDefault("userIds", Lists.newArrayList());
            if (!CollectionUtils.isEmpty(userIds)) {
                sqlBuilder.append(" and broker_user_id in(");
                sqlBuilder.append(String.join(",", userIds.stream().map(a -> "'" + a + "'").collect(Collectors.toList())));
                sqlBuilder.append(")");
            }

            final Long lastUserId = (Long) parameter.getOrDefault("lastUserId", 0L);
            sqlBuilder.append(" and broker_user_id > ").append("'" + lastUserId + "'");


            sqlBuilder.append(" group by broker_user_id having(value >= #{minAmount} ");
            final BigDecimal maxAmount = (BigDecimal) parameter.getOrDefault("maxAmount", BigDecimal.ZERO);
            if (maxAmount.compareTo(BigDecimal.ZERO) > 0) {
                sqlBuilder.append(" and value < #{maxAmount} ");
            }
            sqlBuilder.append(") ");

            sqlBuilder.append(" order by broker_user_id asc ");


            final Integer limit = (Integer) parameter.getOrDefault("limit", 0);
            if (limit > 0) {
                sqlBuilder.append(" limit ").append(limit);
            }
            log.info("sql:{}",sqlBuilder.toString());
            return sqlBuilder.toString();
        }


        public String tradeAmountBtcByAccount(Map<String, Object> parameter) {
            StringBuilder sqlBuilder = new StringBuilder("select ifnull(sum(buy_amount_btc + sell_amount_btc), 0) amount from rpt_account_trade_summary_day ");
            sqlBuilder.append(" where broker_id = #{brokerId} and dt between #{startDate} and #{endDate} ");
           // sqlBuilder.append(" and buy_fee_token not in('TBTC', 'TUSDT', 'TETH', 'BUSDT', 'CBTC')  and sell_fee_token not in('TBTC', 'TUSDT', 'TETH', 'BUSDT', 'CBTC')");

            final List<Long> accountIds = (List<Long>) parameter.getOrDefault("accountIds", Lists.newArrayList());
            if (!CollectionUtils.isEmpty(accountIds)) {
                sqlBuilder.append(" and account_id in(");
                for (int i = 0; i < accountIds.size(); i++) {
                    sqlBuilder.append(String.format("#{accountIds[%d]}", i));
                    if (i < accountIds.size() - 1) {
                        sqlBuilder.append(",");
                    }
                }
                sqlBuilder.append(")");
            }

            return sqlBuilder.toString();
        }

    }
}

