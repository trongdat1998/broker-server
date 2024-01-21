package io.bhex.broker.server.grpc.server.service;

import com.google.protobuf.TextFormat;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.*;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenFuturesInfo;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.order.FuturesOrderSide;
import io.bhex.broker.grpc.order.FuturesPriceType;
import io.bhex.broker.server.grpc.client.service.GrpcQuoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class FuturesQuoteService {

    @Resource
    GrpcQuoteService grpcQuoteService;

    @Resource
    BasicService basicService;


    public BigDecimal getCurrentPrice(String symbolId, Long exchangeId, Long orgId) {
        GetRealtimeReply reply = grpcQuoteService.getRealtime(exchangeId, symbolId, orgId);
        if (Objects.nonNull(reply) && CollectionUtils.isNotEmpty(reply.getRealtimeList())) {
            Realtime realtime = reply.getRealtime(0);
            return StringUtils.isNotEmpty(realtime.getC()) ? new BigDecimal(realtime.getC()) : BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(0);
    }

    public BigDecimal getIndices(String indexToken, Long orgId) {
        GetIndicesReply getIndicesReply = grpcQuoteService.getIndices(indexToken, orgId);
        if (Objects.isNull(getIndicesReply) || Objects.isNull(getIndicesReply.getIndicesMapMap())) {
            return BigDecimal.ZERO;
        }
        Map<String, Index> map = getIndicesReply.getIndicesMapMap();
        if (map.get(indexToken) == null) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(map.get(indexToken).getIndex().getStr());
    }

    public Depth getDepth(Long exchangeId, String symbol, Long orgId) {
        int dumpScale = 18;
        int limitCount = 100;
        GetDepthReply reply = grpcQuoteService.getPartialDepth(exchangeId, symbol, dumpScale, limitCount, orgId);
        if (reply == null || CollectionUtils.isEmpty(reply.getDepthList())) {
            log.warn("getDepth is null. exchangeId:{}, symbol:{}", exchangeId, symbol);
            return null;
        }
        Depth depth = reply.getDepthList().get(0);

        /**
         * depth.getAsks();//100 - top1 sell
         * depth.getBids();//100 - top1 buy
         */
        log.debug("getDepth. depth:{}", TextFormat.shortDebugString(depth));
        return depth;
    }

    private BigDecimal opponentPrice(FuturesOrderSide orderSide, Long exchangeId, String symbolId, Long orgId) {
        try {
            //买1卖1
            Depth depth = getDepth(exchangeId, symbolId, orgId);
            if (depth == null) {
                return null;
            }
            //queue price
            if (orderSide == FuturesOrderSide.BUY_OPEN || orderSide == FuturesOrderSide.BUY_CLOSE) {
                if (Objects.isNull(depth.getAsks()) || CollectionUtils.isEmpty(depth.getAsks().getBookOrderList()) ) {
                    return null;
                }
                BigDecimal sellPrice = DecimalUtil.toBigDecimal(depth.getAsks().getBookOrderList().get(0).getPrice());
                return sellPrice;
            }

            if (orderSide == FuturesOrderSide.SELL_OPEN || orderSide == FuturesOrderSide.SELL_CLOSE) {
                if (Objects.isNull(depth.getBids()) || CollectionUtils.isEmpty(depth.getBids().getBookOrderList())) {
                    return null;
                }
                BigDecimal buyPrice = DecimalUtil.toBigDecimal(depth.getBids().getBookOrderList().get(0).getPrice());
                return buyPrice;
            }
        } catch (Exception e) {
            log.error("opponentPrice error", e);
        }
        return null;
    }

    private BigDecimal queuePrice(FuturesOrderSide orderSide, Long exchangeId, String symbolId, Long orgId) {
        try {
            //买1卖1
            Depth depth = getDepth(exchangeId, symbolId, orgId);
            //queue price
            if (orderSide == FuturesOrderSide.BUY_OPEN || orderSide == FuturesOrderSide.BUY_CLOSE) {
                if (Objects.isNull(depth.getBids()) || CollectionUtils.isEmpty(depth.getBids().getBookOrderList())) {
                    return null;
                }
                BigDecimal buyPrice = DecimalUtil.toBigDecimal(depth.getBids().getBookOrderList().get(0).getPrice());
                return buyPrice;
            }

            if (orderSide == FuturesOrderSide.SELL_OPEN || orderSide == FuturesOrderSide.SELL_CLOSE) {
                if (Objects.isNull(depth.getAsks()) || CollectionUtils.isEmpty(depth.getAsks().getBookOrderList()) ) {
                    return null;
                }
                BigDecimal sellPrice = DecimalUtil.toBigDecimal(depth.getAsks().getBookOrderList().get(0).getPrice());
                return sellPrice;
            }
        } catch (Exception e) {
            log.error("queuePrice error", e);
        }
        return null;
    }

    private BigDecimal overPrice(FuturesOrderSide orderSide, Long exchangeId, String symbolId, Long orgId) {
        try {
            //买1卖1
            Depth depth = getDepth(exchangeId, symbolId, orgId);

            //浮动额
            List<String> overPriceRanges = getOverPriceRanges(symbolId);
            if (overPriceRanges == null) {
                return null;
            }
            BigDecimal minRate = new BigDecimal(overPriceRanges.get(0));
            BigDecimal maxRate = new BigDecimal(overPriceRanges.get(1));

            //over price
            if (orderSide == FuturesOrderSide.BUY_OPEN || orderSide == FuturesOrderSide.BUY_CLOSE) {
                if (Objects.isNull(depth.getAsks()) || CollectionUtils.isEmpty(depth.getAsks().getBookOrderList()) ) {
                    return null;
                }
                BigDecimal sellPrice = DecimalUtil.toBigDecimal(depth.getAsks().getBookOrderList().get(0).getPrice());
                return sellPrice.add(maxRate); //卖1价格 + max浮动额
            }

            if (orderSide == FuturesOrderSide.SELL_OPEN || orderSide == FuturesOrderSide.SELL_CLOSE) {
                if (Objects.isNull(depth.getBids()) || CollectionUtils.isEmpty(depth.getBids().getBookOrderList())) {
                    return null;
                }
                BigDecimal buyPrice = DecimalUtil.toBigDecimal(depth.getBids().getBookOrderList().get(0).getPrice());
                return buyPrice.add(minRate); //买1价格 + min浮动额
            }
        } catch (Exception e) {
            log.error("overPrice error", e);
        }
        return null;
    }

    public BigDecimal marketPrice(FuturesOrderSide orderSide, String symbolId, Long exchangeId, Long orgId) {
        try {
            //浮动额
            List<String> marketPriceRanges = getMarketPriceRanges(symbolId);
            if (marketPriceRanges == null) return null;
            BigDecimal minRate = new BigDecimal(marketPriceRanges.get(0));
            BigDecimal maxRate = new BigDecimal(marketPriceRanges.get(1));

            //当前价
            GetRealtimeReply reply = grpcQuoteService.getRealtime(exchangeId, symbolId, orgId);
            if (reply == null || CollectionUtils.isEmpty(reply.getRealtimeList())) {
                return null;
            }
            Realtime realtime = reply.getRealtime(0);
            if (StringUtils.isEmpty(realtime.getC())) {
                return null;
            }

            //market price
            BigDecimal price = new BigDecimal(realtime.getC());
            if (orderSide == FuturesOrderSide.BUY_OPEN || orderSide == FuturesOrderSide.BUY_CLOSE) {
                return price.multiply(BigDecimal.ONE.add(maxRate));//最新价*（1+5%）
            }
            if (orderSide == FuturesOrderSide.SELL_OPEN || orderSide == FuturesOrderSide.SELL_CLOSE) {
                return price.multiply(BigDecimal.ONE.add(minRate));//最新价*（1-5%）
            }
        } catch (Exception e) {
            log.error("marketPrice error", e);
        }
        return null;
    }

    /**
     * 新的市价策略：使用对手价+浮动范围
     */
    public BigDecimal newMarketPrice(FuturesOrderSide orderSide, String symbolId, Long exchangeId, Long orgId) {
        try {
            // 买1卖1
            Depth depth = getDepth(exchangeId, symbolId, orgId);

            // 浮动额
            List<String> marketPriceRanges = getMarketPriceRanges(symbolId);
            if (marketPriceRanges == null) return null;
            BigDecimal minRate = new BigDecimal(marketPriceRanges.get(0));
            BigDecimal maxRate = new BigDecimal(marketPriceRanges.get(1));

            // market price
            if (orderSide == FuturesOrderSide.BUY_OPEN || orderSide == FuturesOrderSide.BUY_CLOSE) {
                if (Objects.isNull(depth.getAsks()) || CollectionUtils.isEmpty(depth.getAsks().getBookOrderList()) ) {
                    return null;
                }
                BigDecimal sellPrice = DecimalUtil.toBigDecimal(depth.getAsks().getBookOrderList().get(0).getPrice());
                return sellPrice.multiply(BigDecimal.ONE.add(maxRate)); //卖1价格 + max浮动比例
            }

            if (orderSide == FuturesOrderSide.SELL_OPEN || orderSide == FuturesOrderSide.SELL_CLOSE) {
                if (Objects.isNull(depth.getBids()) || CollectionUtils.isEmpty(depth.getBids().getBookOrderList())) {
                    return null;
                }
                BigDecimal buyPrice = DecimalUtil.toBigDecimal(depth.getBids().getBookOrderList().get(0).getPrice());
                return buyPrice.multiply(BigDecimal.ONE.add(minRate)); //卖1价格 + max浮动比例
            }
        } catch (Exception e) {
            log.error("newMarketPrice error", e);
        }
        return null;
    }

    private List<String> getOverPriceRanges(String symbolId) {
        try {
            TokenFuturesInfo futuresInfo = basicService.getTokenFuturesInfoMap().get(symbolId);
            if (futuresInfo == null || StringUtils.isEmpty(futuresInfo.getOverPriceRange())) {
                return null;
            }
            List<String> overPriceRanges = Arrays.asList(futuresInfo.getOverPriceRange().split(","));
            if (CollectionUtils.isEmpty(overPriceRanges) || overPriceRanges.size() < 2) {
                return null;
            }
            return overPriceRanges;
        } catch (Exception e) {
            log.error("getOverPriceRanges error", e);
        }
        return null;

    }

    private List<String> getMarketPriceRanges(String symbolId) {
        try {
            TokenFuturesInfo futuresInfo = basicService.getTokenFuturesInfoMap().get(symbolId);
            if (futuresInfo == null || StringUtils.isEmpty(futuresInfo.getMarketPriceRange())) {
                return null;
            }
            List<String> marketPriceRanges = Arrays.asList(futuresInfo.getMarketPriceRange().split(","));
            if (CollectionUtils.isEmpty(marketPriceRanges) || marketPriceRanges.size() < 2) {
                return null;
            }
            return marketPriceRanges;
        } catch (Exception e) {
            log.error("getMarketPriceRanges error", e);
        }
        return null;
    }

    /**
     * 获取期货下单价格
     * <p>
     * 规则：https://wiki.bhex.io/pages/viewpage.action?pageId=5053674
     *
     * @param orderSide    order side
     * @param exchangeId   exchange id
     * @param symbol     symbol detail
     * @param priceType    价格类型
     * @param price        用户输入的价格
     * @param triggerPrice 委托单触发价
     * @param isClose 是否平仓单
     * @return 实际下单价
     */
    public BigDecimal getFutureOrderPrice(FuturesOrderSide orderSide, Long exchangeId, SymbolDetail symbol,
                                          FuturesPriceType priceType, BigDecimal price, BigDecimal triggerPrice, boolean isClose, Long orgId) {
        if (priceType == FuturesPriceType.INPUT) {//用户输入价
            return price;
        }
        String symbolId = symbol.getSymbolId();
        int symbolPrecision = DecimalUtil.toBigDecimal(symbol.getMinPricePrecision()).stripTrailingZeros().scale();
        price = price.setScale(symbolPrecision, BigDecimal.ROUND_DOWN);
        if (priceType == FuturesPriceType.OPPONENT) {//对手价
            if (price != null && price.compareTo(BigDecimal.ZERO) > 0) {
                return price;
            }
            BigDecimal opponentPrice = opponentPrice(orderSide, exchangeId, symbolId, orgId);
            checkPriceIfNull(exchangeId, symbolId, opponentPrice, "opponentPrice is null/zero. exchangeId:{}, symbolId:{}");
            return opponentPrice.setScale(symbolPrecision, BigDecimal.ROUND_DOWN);
        }
        if (priceType == FuturesPriceType.QUEUE) {//排队价
            if (price != null && price.compareTo(BigDecimal.ZERO) > 0) {
                return price;
            }
            BigDecimal queuePrice = queuePrice(orderSide, exchangeId, symbolId, orgId);
            checkPriceIfNull(exchangeId, symbolId, queuePrice, "queuePrice is null/zero. exchangeId:{}, symbolId:{}");
            return queuePrice.setScale(symbolPrecision, BigDecimal.ROUND_DOWN);
        }
        if (priceType == FuturesPriceType.OVER) {//超价
            if (price != null && price.compareTo(BigDecimal.ZERO) > 0) {
                return price;
            }
            BigDecimal overPrice = overPrice(orderSide, exchangeId, symbolId, orgId);
            checkPriceIfNull(exchangeId, symbolId, overPrice, "overPrice is null/zero. exchangeId:{}, symbolId:{}");
            return overPrice.setScale(symbolPrecision, BigDecimal.ROUND_DOWN);
        }
        if (priceType == FuturesPriceType.MARKET_PRICE) {//市价
            if ((price != null && price.compareTo(BigDecimal.ZERO) > 0) || isClose) { // 如果是市价平仓单，不用取市价价格
                return price;
            }
            BigDecimal marketPrice = newMarketPrice(orderSide, symbolId, exchangeId, orgId);
            checkPriceIfNull(exchangeId, symbolId, marketPrice, "marketPrice is null/zero. exchangeId:{}, symbolId:{}");
            return marketPrice.setScale(symbolPrecision, BigDecimal.ROUND_DOWN);
        }
        log.warn("getFutureOrderPrice error. exchangeId:{}, symbolId:{}, price:{}", exchangeId, symbolId, price);
        throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
    }

    private void checkPriceIfNull(Long exchangeId, String symbolId, BigDecimal price, String s) {
        if (price == null || price.compareTo(BigDecimal.ZERO) <= 0) {
            log.error(s, exchangeId, symbolId);
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
    }

    private FuturesOrderSide reverseOrderSide(FuturesOrderSide orderSide) {
        if (orderSide == FuturesOrderSide.BUY_OPEN) {
            return FuturesOrderSide.SELL_OPEN;
        } else if (orderSide == FuturesOrderSide.SELL_OPEN) {
            return FuturesOrderSide.BUY_OPEN;
        } else if (orderSide == FuturesOrderSide.BUY_CLOSE) {
            return FuturesOrderSide.BUY_CLOSE;
        } else if (orderSide == FuturesOrderSide.SELL_CLOSE) {
            return FuturesOrderSide.SELL_CLOSE;
        }
        return FuturesOrderSide.UNKNOWN_FUTURES_ORDER_SIDE;
    }

}
