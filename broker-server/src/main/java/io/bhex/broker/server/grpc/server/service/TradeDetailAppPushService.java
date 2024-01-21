package io.bhex.broker.server.grpc.server.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import io.bhex.base.account.GetOrderReply;
import io.bhex.base.account.GetOrderRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.proto.OrderTypeEnum;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenCategory;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.order.GetFuturesI18nNameRequest;
import io.bhex.broker.grpc.order.GetFuturesI18nNameResponse;
import io.bhex.broker.grpc.order.Order;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.client.service.GrpcFuturesOrderService;
import io.bhex.broker.server.grpc.client.service.GrpcOrderService;
import io.bhex.broker.server.message.MessageTradeDetail;
import io.bhex.broker.server.message.consumer.TradeDetailConsumer;
import io.bhex.broker.server.model.AppPushDevice;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class TradeDetailAppPushService {
    @Autowired
    private NoticeTemplateService noticeTemplateService;
    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;
    @Resource
    private SymbolMapper symbolMapper;
    @Resource
    private GrpcOrderService grpcOrderService;
    @Resource
    GrpcFuturesOrderService grpcFuturesOrderService;
    @Resource
    FuturesOrderService futuresOrderService;
    @Autowired
    private BaseBizConfigService baseBizConfigService;
    @Resource
    private TradeDetailConsumer tradeDetailConsumer;

    @Resource
    private AppPushService appPushService;
    @Resource
    private BasicService basicService;

    private static final List<String> TEST_TOKENS = Arrays.asList(AccountService.TEST_TOKENS);
    private static final Long TIME_GAP_ORDER_CREADTED = 180_000L;
    private static final Long REPATED_PUSH_TIME_GAP = 180_000L;
    private static final Cache<Long, Set<Long>> BLACK_ACCOUNT_CACHE =
            CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
    private static final Cache<Long, Set<Long>> BLACK_USER_CACHE =
            CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
    private static final Cache<Long, Set<String>> BLACK_SYMBOL_CACHE =
            CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
    private static final Cache<String, String> SYMBOL_NAME_CACHE =
            CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.MINUTES).build();

    @Scheduled(cron = "0/10 * * * * ?")
    public void refreshTradeDetailConsumer() {

        List<BaseConfigInfo> configInfos = baseBizConfigService.getConfigsByGroupsAndKey(0L,
                Lists.newArrayList(BaseConfigConstants.APPPUSH_CONFIG_GROUP), "ALL_SITE");
        if (CollectionUtils.isEmpty(configInfos)) {
            return;
        }
        configInfos.forEach(c -> tradeDetailConsumer.bootTradeDetailConsumer("_TRADE_DETAIL_APP_PUSH", c.getOrgId(),
                tradeDetail -> {
                    //超过5秒的撮合消息，直接丢弃
                    if (System.currentTimeMillis() - tradeDetail.getMatchTime() > 5000) {
                        return true;
                    }
                    return handleTradeData(tradeDetail);
                })
        );
    }

    public Boolean handleTradeData(MessageTradeDetail tradeDetail) {
        if (TEST_TOKENS.contains(tradeDetail.getBaseTokenId()) || TEST_TOKENS.contains(tradeDetail.getQuoteTokenId())) {
            return Boolean.TRUE;
        }
        if (tradeDetail.getBrokerUserId() == 0 || tradeDetail.getAccountId() == 0) {
            return Boolean.TRUE;
        }
        OrderTypeEnum orderTypeEnum = OrderTypeEnum.forNumber(tradeDetail.getOrderType());
        if (orderTypeEnum == null || !orderTypeEnum.name().startsWith("LIMIT")) {
            return Boolean.TRUE;
        }
        String lockKey = "trade_detail_app_push_" + tradeDetail.getBrokerUserId() + "_" + tradeDetail.getSymbolId();
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, REPATED_PUSH_TIME_GAP);
        if (!lock) { //3分钟单币对发一条
            return Boolean.TRUE;
        }

        long orgId = tradeDetail.getOrgId();

        Header header = Header.newBuilder().setOrgId(orgId).setUserId(tradeDetail.getBrokerUserId()).build();
        JsonObject jsonObject = new JsonObject();
        io.bhex.broker.grpc.basic.Symbol symbol = basicService.getOrgSymbol(tradeDetail.getOrgId(), tradeDetail.getSymbolId());
        if (symbol == null || symbol.getCategory() != TokenCategory.MAIN_CATEGORY_VALUE && symbol.getCategory() != TokenCategory.FUTURE_CATEGORY_VALUE) {
            return Boolean.TRUE;
        }

        NoticeBusinessType businessType = null;
        boolean isContract = symbol.getCategory() == TokenCategory.FUTURE_CATEGORY_VALUE;
        if (!isContract) {
            jsonObject.addProperty("price", tradeDetail.getPrice().stripTrailingZeros().toPlainString());
            jsonObject.addProperty("symbol", symbol.getSymbolName());
            businessType = tradeDetail.getSide() == OrderSideEnum.BUY_VALUE
                    ? NoticeBusinessType.SPOT_BUY_TRADE_SUCCESS : NoticeBusinessType.SPOT_SELL_TRADE_SUCCESS;
            GetOrderRequest request = GetOrderRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                    .setAccountId(tradeDetail.getAccountId())
                    .setOrderId(tradeDetail.getOrderId())
                    .build();
            GetOrderReply reply = grpcOrderService.getOrderInfo(request);
            if (System.currentTimeMillis() - reply.getOrder().getCreatedTime() < TIME_GAP_ORDER_CREADTED) { //如果订单在一分钟内成交 就不发了
                return Boolean.TRUE;
            }
        } else {
            SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(orgId, tradeDetail.getSymbolId());
            if (symbolDetail == null) {
                return Boolean.TRUE;
            }
            io.bhex.base.account.GetOrderRequest request = GetOrderRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequestByHeader(header))
                    .setAccountId(tradeDetail.getAccountId())
                    .setOrderId(tradeDetail.getOrderId())
                    .build();
            GetOrderReply reply = grpcFuturesOrderService.getFuturesOrderInfo(request);
            if (System.currentTimeMillis() - reply.getOrder().getCreatedTime() < TIME_GAP_ORDER_CREADTED) { //如果订单在一分钟内成交 就不发了
                return Boolean.TRUE;
            }

            String priceVal;
            int minPricePrecision = DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().scale();
            if (tradeDetail.getPrice().compareTo(BigDecimal.ZERO) > 0) {
                priceVal = symbolDetail.getIsReverse()
                        ? BigDecimal.ONE.divide(tradeDetail.getPrice(), minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString()
                        : tradeDetail.getPrice().setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
            } else {
                priceVal = tradeDetail.getPrice().setScale(minPricePrecision, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
            }

            Order futureOrder = futuresOrderService.getFuturesOrder(reply.getOrder(), symbolDetail, symbolDetail.getExchangeId());
            jsonObject.addProperty("orderMargin", futureOrder.getOrderMarginLocked());
            jsonObject.addProperty("leverage", futureOrder.getLeverage());
            jsonObject.addProperty("price", priceVal);


            AppPushDevice pushDevice = appPushService.getPushDevice(tradeDetail.getBrokerUserId());
            //log.info("push:{}", pushDevice);
            if (pushDevice == null || StringUtils.isEmpty(pushDevice.getLanguage()) || !pushDevice.getLanguage().contains("_")) {
                return Boolean.TRUE;
            }

            jsonObject.addProperty("symbol", getFutureSymbolNameI18(tradeDetail.getSymbolId(), pushDevice.getLanguage()));
            //log.info("push:{}", jsonObject);
            Order.FuturesOrderSide orderSide = futureOrder.getFuturesOrderSide();
            switch (orderSide) {
                case BUY_OPEN:
                    businessType = NoticeBusinessType.CONTRACT_BUY_OPEN_TRADE_SUCCESS;
                    break;
                case BUY_CLOSE:
                    businessType = NoticeBusinessType.CONTRACT_BUY_CLOSE_TRADE_SUCCESS;
                    break;
                case SELL_OPEN:
                    businessType = NoticeBusinessType.CONTRACT_SELL_OPEN_TRADE_SUCCESS;
                    break;
                case SELL_CLOSE:
                    businessType = NoticeBusinessType.CONTRACT_SELL_CLOSE_TRADE_SUCCESS;
                    break;
            }
        }

        if (businessType == null || !noticeTemplateService.canSendPush(orgId, tradeDetail.getBrokerUserId(), businessType)) {
            return Boolean.TRUE;
        }


        jsonObject.addProperty("quantity", tradeDetail.getQuantity().stripTrailingZeros().toPlainString());
        jsonObject.addProperty("amount", tradeDetail.getAmount().stripTrailingZeros().toPlainString());


        String orderType = "COIN";
        if (symbol.getCategory() == TokenCategory.FUTURE_CATEGORY_VALUE) {
            orderType = "FUTURES";
        }


        Map<String, String> pushUrlData = Maps.newHashMap();
        pushUrlData.put("SYMBOL_ID", tradeDetail.getSymbolId());
        pushUrlData.put("ORDER_ID", tradeDetail.getOrderId() + "");
        pushUrlData.put("ORDER_TYPE", orderType);

        // pushUrlData.put("ORDER_DIR", tradeDetail.getSide() == OrderSideEnum.BUY_VALUE ? "BUY" : "SELL");
        log.info("tradeDetailAppPush:{}", tradeDetail);
        noticeTemplateService.sendBizPushNotice(header, tradeDetail.getBrokerUserId(), businessType,
                tradeDetail.getTradeId() + "", jsonObject, pushUrlData);
        return true;
    }

    private String getFutureSymbolNameI18(String symbolId, String language) {
        try {
            String symbolName = SYMBOL_NAME_CACHE.get(symbolId, () -> {
                GetFuturesI18nNameResponse nameResponse = futuresOrderService.getFuturesI18nName(GetFuturesI18nNameRequest.newBuilder()
                        .setInKey(symbolId)
                        .setEnv(language.split("_")[0])
                        .build());
                return nameResponse.getInValue();
            });
            return symbolName;
        } catch (Exception e) {
            log.error("getFutureSymbolNameI18 error {}", symbolId, e);
        }
        return symbolId;
    }
}
