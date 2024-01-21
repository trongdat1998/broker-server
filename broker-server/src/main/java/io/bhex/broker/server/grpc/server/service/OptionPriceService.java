package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.server.domain.BrokerServerConstants;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.Objects;

import javax.annotation.Resource;

import io.bhex.base.quote.GetIndicesReply;
import io.bhex.base.quote.GetRealtimeReply;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.server.grpc.client.service.GrpcQuoteService;
import lombok.extern.slf4j.Slf4j;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 18/01/2019 5:35 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Service
@Slf4j
public class OptionPriceService {

    @Resource
    private GrpcQuoteService grpcQuoteService;

    // 判断是否为做多期权
    private static Boolean isLongOption(BigDecimal total) {
        // 持仓为正则是做多期权
        if (total.compareTo(DecimalUtil.toBigDecimal(0L)) >= 0) {
            return true;
        } else {
            return false;
        }
    }

    // 持仓盈亏 百分比数值
    public BigDecimal getProfitPercentage(BigDecimal total, BigDecimal cost, BigDecimal margin, BigDecimal currentOptionPrice) {
        //   括号里的百分比回报公式：
        //      当方向=做多
        //        持仓盈亏 / （持仓均价 * 持仓量）
        //      当方向=做空
        //        持仓盈亏/ （保证金 - 持仓均价 * 持仓量）
        BigDecimal profit = getProfit(total, cost, currentOptionPrice);
        BigDecimal percentage;
        if (isLongOption(total)) {
            if (profit.compareTo(DecimalUtil.toBigDecimal(0L)) == 0 || cost.compareTo(DecimalUtil.toBigDecimal(0L)) == 0) {
                percentage = BigDecimal.valueOf(0);
            } else {
                percentage = profit.divide(cost.multiply(total.abs()), BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN);
            }
        } else {
            BigDecimal division = margin.subtract(cost.multiply(total.abs()));
            if (profit.compareTo(DecimalUtil.toBigDecimal(0L)) == 0 || division.compareTo(DecimalUtil.toBigDecimal(0L)) == 0) {
                percentage = BigDecimal.valueOf(0);
            } else {
                percentage = profit.divide(division, BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN);
            }
        }
        // 百分比 要乘以100
        return percentage.multiply(BigDecimal.valueOf(100));
    }

    // 持仓盈亏
    public BigDecimal getProfit(BigDecimal total, BigDecimal cost, BigDecimal currentOptionPrice) {
        // 持仓盈亏:
        //   当方向=做多
        //     （最新价 - 持仓均价）*持仓量
        //   当方向=做空
        //     （最新价 - 持仓均价）*持仓量 * （-1）
        BigDecimal currentTotal = currentOptionPrice.multiply(total.abs());
        BigDecimal profit = currentTotal.subtract(cost.multiply(total.abs()));
        // 如果为做空 则取负数
        if (!isLongOption(total)) {
            profit = profit.multiply(BigDecimal.valueOf(-1));
        }
        return profit;
    }

    // 期权估值
    public BigDecimal getValuation(BigDecimal total, BigDecimal currentOptionPrice) {
        return total.abs().multiply(currentOptionPrice);
    }

    // 仓位权益
    public BigDecimal getEquity(BigDecimal total, BigDecimal margin, BigDecimal currentOptionPrice) {
        // 仓位权益:
        //   1、做多：账户权益=最新价*持仓量
        //   2、做空：账户权益=保证金-最新价*持仓量
        BigDecimal equity;

        BigDecimal valuation = getValuation(total, currentOptionPrice);
        if (isLongOption(total)) {
            equity = valuation;
        } else {
            equity = margin.subtract(valuation);
        }
        return equity;
    }

    // 获取当前价格
    public BigDecimal getCurrentOptionPrice(String symbolId, Long exchangeId, Long orgId) {
        GetRealtimeReply realtimeReply = grpcQuoteService.getRealtime(exchangeId, symbolId, orgId);
        if (Objects.nonNull(realtimeReply) && !CollectionUtils.isEmpty(realtimeReply.getRealtimeList())) {
            Realtime realtime = realtimeReply.getRealtime(0);
            BigDecimal currentPrice = new BigDecimal(realtime.getC());
            // log.info("currentPrice {}", currentPrice);
            if (Objects.nonNull(currentPrice)) {
                return currentPrice;
            }
        }
        return BigDecimal.valueOf(0);
    }

    /**
     * 获取对应标的的指数
     *
     * @param symbolId symbolId
     * @return bigDecimal
     */
    public BigDecimal getIndices(String symbolId, Long orgId) {
        GetIndicesReply getIndicesReply = grpcQuoteService.getIndices(symbolId, orgId);
        if (Objects.nonNull(getIndicesReply) && getIndicesReply.getIndicesMapMap() != null) {
            if (getIndicesReply.getIndicesMapMap().get(symbolId) != null) {
                BigDecimal indices = new BigDecimal(getIndicesReply.getIndicesMapMap().get(symbolId).getIndex().getStr());
                // log.info("symbolId {} indices {}", symbolId, indices);
                if (Objects.nonNull(indices)) {
                    return indices;
                }
            } else {
                return new BigDecimal("0.00");
            }
        }
        return new BigDecimal("0.00");
    }
}
