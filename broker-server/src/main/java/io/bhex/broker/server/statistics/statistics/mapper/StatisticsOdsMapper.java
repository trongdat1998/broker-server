package io.bhex.broker.server.statistics.statistics.mapper;

import io.bhex.broker.server.domain.StatisticsAccountTradeSummaryDay;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date: 2019/11/1 上午10:22
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface StatisticsOdsMapper {

    //sql like select org_id, count(*) as total ,a,b,c from tb_user group by org_id where created between {start} and {end}
    @Select("${sql}")
    List<HashMap<String, Object>> queryGroupDataList(@Param("sql") String sql);

    @Select("select account_id as accountId,token_id tokenId,sum(amount) amount, sum(amount_usd) usdtAmount from dws_deposit_day "
            + " where date between #{item.startDate} and #{item.endDate} and broker_id = #{item.brokerId} and token_id = #{item.tokenId} group by account_id "
            + " order by amount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> dwTopDepositToken(@Param("item") Map<String, Object> item);

    @Select("select account_id as accountId,sum(amount_usd) usdtAmount from rpt_user_deposit_day where date between #{item.startDate} and #{item.endDate} "
            + " and broker_id = #{item.brokerId} group by account_id order by usdtAmount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> dwTopDepositAccountSum(@Param("item") Map<String, Object> item);

    @Select("select account_id as accountId,token_id tokenId,sum(amount) amount,sum(amount_usd) usdtAmount from dws_withdrawal_day "
            + " where date between #{item.startDate} and #{item.endDate} and broker_id = #{item.brokerId} and token_id = #{item.tokenId} "
            + "  group by account_id order by amount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> dwTopWithdrawalToken(@Param("item") Map<String, Object> item);

    @Select("select account_id as accountId,sum(amount_usd) usdtAmount from rpt_user_withdraw_day where date between #{item.startDate} and #{item.endDate} "
            + " and broker_id = #{item.brokerId} group by account_id order by usdtAmount desc  limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> dwTopWithdrawalAccountSum(@Param("item") Map<String, Object> item);


    @Select("select sum(buy_order_num+sell_order_num) as num,account_id as accountId,sum(buy_order_num) buyNum,sum(sell_order_num) sellNum,"
            + "sum(buy_quantity) buyQuantity,sum(sell_quantity) sellQuantity from rpt_account_trade_summary_day where dt between #{item.startDate} and #{item.endDate} "
            + " and broker_id = #{item.brokerId} and trade_type = #{item.tradeType} and symbol_id = #{item.symbolId} "
            + "  group by account_id order by 1 desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> dwTopTradeNumSymbol(@Param("item") Map<String, Object> item);


    @Select("select sum(buy_order_num+sell_order_num) as num,account_id as accountId,sum(buy_order_num) buyNum,"
            + " sum(sell_order_num) sellNum,sum(buy_quantity) buyQuantity,sum(sell_quantity) sellQuantity from rpt_account_trade_summary_day"
            + " where dt between #{item.startDate} and #{item.endDate}  and broker_id = #{item.brokerId} and trade_type = #{item.tradeType} "
            + "  group by account_id order by 1 desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> dwTopTradeNumAccountSum(@Param("item") Map<String, Object> item);


    @Select("select sum(buy_quantity+sell_quantity) as quantity, account_id as accountId,sum(buy_order_num) buyNum,sum(sell_order_num) sellNum,"
            + " sum(buy_quantity) buyQuantity,sum(sell_quantity) sellQuantity from rpt_account_trade_summary_day where dt between #{item.startDate} and #{item.endDate} "
            + " and broker_id = #{item.brokerId} and trade_type = #{item.tradeType} and symbol_id = #{item.symbolId} "
            + "  group by account_id order by 1 desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> dwTopTradeQuantitySymbol(@Param("item") Map<String, Object> item);


    @Select("select account_id as accountId,#{item.tokenId} tokenId,"
            + "sum(if(buy_fee_token=#{item.tokenId}, buy_fee_total,0) + if(sell_fee_token=#{item.tokenId}, sell_fee_total,0)) as feeAmount from rpt_account_trade_summary_day "
            + " where dt between #{item.startDate} and #{item.endDate} and broker_id = #{item.brokerId} and trade_type = 1"
            + " and (sell_fee_token = #{item.tokenId} or buy_fee_token = #{item.tokenId})"
            + "  group by account_id order by feeAmount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> tradeFeeTopSpotToken(@Param("item") Map<String, Object> item);


    @Select("select account_id as accountId,sum(total_fee_usd) feeUsdtAmount from rpt_user_fee_spot_trading_day "
            + " where date between #{item.startDate} and #{item.endDate} and broker_id = #{item.brokerId} "
            + " group by account_id order by feeUsdtAmount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> tradeFeeTopSpotAccountSum(@Param("item") Map<String, Object> item);

    @SelectProvider(type = Provider.class, method = "tradeFeeTopBySymbols")
    List<HashMap<String, Object>> tradeFeeTopContractToken(@Param("item") Map<String, Object> item);

    @Select("select account_id as accountId,sum(total_fee_usd) feeUsdtAmount from rpt_user_fee_contract_trading_day "
            + " where date between #{item.startDate} and #{item.endDate} and broker_id = #{item.brokerId} "
            + "  group by account_id order by feeUsdtAmount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> tradeFeeTopContractAccountSum(@Param("item") Map<String, Object> item);


    @Select("select sum(buy_fee_total+sell_fee_total) as feeAmount,sum(buy_fee_total_usdt+sell_fee_total_usdt) as feeUsdtAmount,"
            + "account_id as accountId,sum(buy_order_num) buyNum,sum(sell_order_num) sellNum from rpt_account_trade_summary_day where dt between #{item.startDate} and #{item.endDate} "
            + " and broker_id = #{item.brokerId} and trade_type = 100 and symbol_id = #{item.symbolId} "
            + "  group by account_id order by 1 desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> tradeFeeTopOtcToken(@Param("item") Map<String, Object> item);


    @Select("select sum(buy_fee_total_usdt+sell_fee_total_usdt) as feeUsdtAmount,"
            + " account_id as accountId from rpt_account_trade_summary_day"
            + " where dt between #{item.startDate} and #{item.endDate}  and broker_id = #{item.brokerId} and trade_type = 100 group by account_id "
            + "  group by account_id  order by 1 desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> tradeFeeTopOtcAccountSum(@Param("item") Map<String, Object> item);


    @Select("select account_id as accountId,#{item.tokenId} as tokenId,"
            + "sum(if(buy_fee_token=#{item.tokenId}, buy_fee_total,0) + if(sell_fee_token=#{item.tokenId}, sell_fee_total,0)) as feeAmount from rpt_account_trade_summary_day "
            + " where dt between #{item.startDate} and #{item.endDate} and broker_id = #{item.brokerId}"
            + " and (sell_fee_token = #{item.tokenId} or buy_fee_token = #{item.tokenId})"
            + "  group by account_id order by feeAmount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> tradeFeeTopToken(@Param("item") Map<String, Object> item);

    @Select("select account_id as accountId,sum(buy_fee_total_usdt+sell_fee_total_usdt) as feeUsdtAmount from rpt_account_trade_summary_day "
            + " where dt between #{item.startDate} and #{item.endDate} and broker_id = #{item.brokerId}"
            + "  group by account_id order by feeUsdtAmount desc limit #{item.start},#{item.pageSize}")
    List<HashMap<String, Object>> tradeFeeTopAccountSum(@Param("item") Map<String, Object> item);


    class Provider {
        public String tradeFeeTopBySymbols(Map<String, Map<String, Object>> parameter) {
            final List<String> symbols = (List<String>) parameter.get("item").get("symbols");

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("select sum(buy_fee_total+sell_fee_total) as feeAmount,sum(buy_fee_total_usdt+sell_fee_total_usdt) as feeUsdtAmount,")
                    .append("account_id as accountId,sum(buy_order_num) buyNum,sum(sell_order_num) sellNum ")
                    .append(" from rpt_account_trade_summary_day where dt between #{item.startDate} and #{item.endDate} ")
                    .append(" and broker_id = #{item.brokerId} and trade_type = #{item.tradeType} ")
                    .append(" and symbol_id in(");
            sqlBuilder.append(String.join(",", symbols.stream().map(s -> "'" + s + "'").collect(Collectors.toList())));
            sqlBuilder.append(")").append(" group by account_id order by 1 desc limit #{item.start},#{item.pageSize}");


            return sqlBuilder.toString();
        }
    }

    @Select(" <script> "
            + " SELECT *  FROM rpt_account_trade_summary_day WHERE dt = #{dt} and broker_id = #{brokerId} and account_id in "
            +   " <foreach item='item' index='index' collection='accountIds' open='(' separator=',' close=')'> "
            +       " #{item} "
            +   " </foreach> "
            + " </script> ")
    List<StatisticsAccountTradeSummaryDay> queryTradeSummaryDayByAccountIds(@Param("dt") String dt, @Param("brokerId") Long brokerId, @Param("accountIds") List<Long> accountIds);


    @Select("select * from rpt_account_trade_summary_day where dt = #{dt} and broker_id = #{brokerId} limit 1;")
    StatisticsAccountTradeSummaryDay getOneTradeSummaryDayByOrgId(@Param("dt") String dt, @Param("brokerId") Long brokerId);
}
