package io.bhex.broker.server.push.business;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.*;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.client.service.GrpcQuoteService;
import io.bhex.broker.server.grpc.server.service.AppBusinessPushService;
import io.bhex.broker.server.grpc.server.service.BaseBizConfigService;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.message.consumer.TradeDetailConsumer;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.FavoriteMapper;
import io.bhex.broker.server.push.bo.OrgBusinessPushRecordSimple;
import io.bhex.broker.server.push.bo.RedisStringLockUtil;
import io.bhex.broker.server.push.service.OrgBusinessPushRecordService;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author wangsc
 * @description 行情波动通知（所有自选用户）favorite
 * @date 2020-09-07 16:16
 */
@Slf4j
@Service
public class AppPushTicketNotify {

    private final ThreadPoolExecutor poolExecutor = createThreadPoolExecutor();

    private static final String UNDER_LINE = "_";

    private static final long DAY_TIME_MILLIS = 24 * 3600 * 1000;

    private static final long ONE_HOUR_MILLIS = 3600 * 1000;

    private static final long TEN_MINUTE_MILLIS = 10 * 60 * 1000;

    private static final long FIFTEEN_MINUTE_MILLIS = 15 * 60 * 1000;

    private static final long THIRTY_SECOND_MILLIS = 30 * 1000;

    private final BigDecimal up_ignore = new BigDecimal("0.05");

    private final BigDecimal down_ignore = new BigDecimal("-0.05");

    private static final String REALTIME_LOCK_PRE = "PUSH_REALTIME:";

    /**
     * 本地缓存，同一个段位24小时制发送一次
     * key: orgId + "_" + symbolId + "_" + step
     * value: 时间戳（多节点中任一个节点触发的开始时间）
     */
    private final Cache<String, Long> stepTriggerLimitCache = CacheBuilder.newBuilder()
            .expireAfterWrite(24, TimeUnit.HOURS)
            .build();

    /**
     * 本地缓存，同一个币种10分钟内最多发送一次
     * key: orgId + "_" + symbolId
     * value: 时间戳（多节点中任一个节点触发的开始时间）
     */
    private final Cache<String, Long> symbolTriggerLimitCache = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    /**
     * 缓存24小时内的十五分钟开盘价（用作伪24小时涨跌幅判断）
     * key: orgId + "_" + symbolId
     * value: [小时id,开盘价]  升序
     */
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, BigDecimal>> fifteenMinuteOpenPriceMap = new ConcurrentHashMap<>();

    /**
     * 本地10s内只允许一个参与竞争
     * key: orgId + "_" + symbolId
     */
    private final ConcurrentHashMap<String, Long> preSymbolTriggerMap = new ConcurrentHashMap<>();

    private final ImmutableMap<Integer, BigDecimal> stepImmutableMap = getStepImmutableMap();

    private final ImmutableSet<String> fixedSymbolSet = ImmutableSet.of("BTCUSDT", "ETHUSDT");

    /**
     * 存储orgId的realtimeInterval配置
     * 24h/1d/1d+8
     */
    private ImmutableMap<Long, RealtimeIntervalEnum> orgRealtimeInterval = ImmutableMap.of();

    private static final DateTimeZone GMT = DateTimeZone.forID("Asia/Shanghai");

    @Resource
    private FavoriteMapper favoriteMapper;

    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private GrpcQuoteService grpcQuoteService;

    @Resource
    private BasicService basicService;

    @Resource(name = "pushBusinessLimitRedisTemplate")
    private RedisTemplate<String, Long> pushBusinessLimitRedisTemplate;

    @Resource
    private RedisStringLockUtil redisStringLockUtil;

    @Resource
    private OrgBusinessPushRecordService orgBusinessPushRecordService;

    @Resource
    private AppBusinessPushService appBusinessPushService;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private TradeDetailConsumer tradeDetailConsumer;

    @Scheduled(cron = "0/10 * * * * ?")
    public void refreshTradeDetailConsumer() {

        List<BaseConfigInfo> configInfos = baseBizConfigService.getConfigsByGroupsAndKey(0L,
                Lists.newArrayList(BaseConfigConstants.APPPUSH_CONFIG_GROUP), "ALL_SITE");
        if (org.springframework.util.CollectionUtils.isEmpty(configInfos)) {
            return;
        }
        configInfos.forEach(c -> tradeDetailConsumer.bootTradeDetailConsumer("_TRADE_DETAIL_APP_PUSH_TICKET", c.getOrgId(),
                tradeDetail -> {
                    //超过5秒的撮合消息，直接丢弃
                    if (System.currentTimeMillis() - tradeDetail.getMatchTime() > 5000) {
                        return true;
                    }
                    return receiveTradePrice(tradeDetail.getOrgId(), tradeDetail.getSymbolId(), tradeDetail.getPrice());
                })
        );
    }

    @EventListener(value = {ApplicationReadyEvent.class})
    @Scheduled(initialDelay = 600 * 1000, fixedDelay = 600 * 1000)
    public void checkFavoriteSymbol() {
        //定时加载自选
        List<BaseConfigInfo> configInfos = baseBizConfigService.getConfigsByGroupsAndKey(0L,
                Lists.newArrayList(BaseConfigConstants.APPPUSH_CONFIG_GROUP), "CUSTOM");
        if (CollectionUtils.isEmpty(configInfos)) {
            return;
        }

        List<Broker> brokers = brokerMapper.queryAvailableBroker();
        if (CollectionUtils.isEmpty(brokers)) {
            return;
        }

        Map<Long, RealtimeIntervalEnum> currentOrgRealtimeInterval = new HashMap<>(16);
        brokers.forEach(broker -> currentOrgRealtimeInterval.put(broker.getOrgId(), RealtimeIntervalEnum.intervalOf(broker.getRealtimeInterval())));
        orgRealtimeInterval = ImmutableMap.copyOf(currentOrgRealtimeInterval);

        List<Long> orgs = configInfos.stream()
                .map(BaseConfigInfo::getOrgId)
                .filter(orgId -> brokers.stream().anyMatch(b -> b.getOrgId().equals(orgId)))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(orgs)) {
            return;
        }

        orgs.forEach(orgId -> {
            List<String> list = favoriteMapper.querySymbolIdsByOrgId(orgId);
            Set<String> symbolSet = new HashSet<>(list);
            symbolSet.addAll(fixedSymbolSet);
            for (String symbolId : symbolSet) {
                String symbolKey = orgId + UNDER_LINE + symbolId;
                if (!fifteenMinuteOpenPriceMap.containsKey(symbolKey)) {
                    initSymbolFifteenMinuteOpen(orgId, symbolId);
                }
            }
        });
        //清理缓存,如果超过100根，则删除第一根
        fifteenMinuteOpenPriceMap.values().stream().filter(skipListMap -> skipListMap.size() > 100).forEach(ConcurrentSkipListMap::pollFirstEntry);
        //清理缓存,如果最新时间id超过1天1小时,则清除相应的缓存
        long invalidTime = System.currentTimeMillis() - DAY_TIME_MILLIS - ONE_HOUR_MILLIS;
        fifteenMinuteOpenPriceMap.entrySet().stream()
                .filter(entry -> entry.getValue().size() == 0 || entry.getValue().lastKey() < invalidTime)
                .forEach(entry -> {
                    //清除所有的不能正常释放的缓存
                    preSymbolTriggerMap.remove(entry.getKey());
                    fifteenMinuteOpenPriceMap.remove(entry.getKey());
                });
    }

    private void initSymbolFifteenMinuteOpen(Long brokerId, String symbolId) {
        try {
            Symbol symbol = basicService.getOrgSymbol(brokerId, symbolId);
            if (symbol == null) {
                return;
            }
            GetLatestKLineRequest getLatestKlineRequest = GetLatestKLineRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(brokerId))
                    .setExchangeId(symbol.getExchangeId())
                    .setSymbol(symbolId)
                    .setLimitCount(24 * 4)
                    .setInterval("15m")
                    .build();
            GetKLineReply kLineReply = grpcQuoteService.getLatestKLine(getLatestKlineRequest);
            String symbolKey = brokerId + UNDER_LINE + symbolId;
            ConcurrentSkipListMap<Long, BigDecimal> concurrentSkipListMap = fifteenMinuteOpenPriceMap.getOrDefault(symbolKey, new ConcurrentSkipListMap<>());
            List<KLine> availableKline = kLineReply.getKlineList().stream()
                    .filter(kLine -> DecimalUtil.toBigDecimal(kLine.getOpen()).compareTo(BigDecimal.ZERO) > 0)
                    .collect(Collectors.toList());
            availableKline.forEach(kLine -> concurrentSkipListMap.put(kLine.getId(), DecimalUtil.toBigDecimal(kLine.getOpen())));
            if (concurrentSkipListMap.size() > 0) {
                fifteenMinuteOpenPriceMap.put(symbolKey, concurrentSkipListMap);
            }
        } catch (Exception e) {
            log.error("get symbol fifteen minute kline error!{},{},{}", brokerId, symbolId, e.getMessage());
        }
    }

    private Realtime getCurrentRealtime(Long brokerId, String symbolId) {
        try {
            Symbol symbol = basicService.getOrgSymbol(brokerId, symbolId);
            if (symbol == null) {
                throw new Exception("no org symbol!");
            }
            GetRealtimeReply realtimeReply = grpcQuoteService.getRealtime(symbol.getExchangeId(), symbolId, brokerId);
            return realtimeReply.getRealtimeList().size() > 0 ? realtimeReply.getRealtime(0) : null;
        } catch (Exception e) {
            log.error("get symbol realtime error!{},{},{}", brokerId, symbolId, e.getMessage());
            return null;
        }
    }

    private KLine getCurrentKline(Long brokerId, String symbolId, String interval) {
        try {
            Symbol symbol = basicService.getOrgSymbol(brokerId, symbolId);
            if (symbol == null) {
                throw new Exception("no org symbol!");
            }
            GetLatestKLineRequest getLatestKlineRequest = GetLatestKLineRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(brokerId))
                    .setExchangeId(symbol.getExchangeId())
                    .setSymbol(symbolId)
                    .setLimitCount(1)
                    .setInterval(interval)
                    .build();
            GetKLineReply kLineReply = grpcQuoteService.getLatestKLine(getLatestKlineRequest);
            return kLineReply.getKlineList().size() > 0 ? kLineReply.getKline(0) : null;
        } catch (Exception e) {
            log.error("get symbol current kline error!{},{},{},{}", brokerId, symbolId, interval, e.getMessage());
            return null;
        }
    }

    private ThreadPoolExecutor createThreadPoolExecutor() {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(5, 5,
                120L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1000),
                new BasicThreadFactory.Builder().namingPattern("PushTicketNotifyTask" + "_%d").build(), (r, executor) -> log.error("PushTicketNotifyTask discard ..........!"));
        poolExecutor.allowCoreThreadTimeOut(true);
        return poolExecutor;
    }

    private ImmutableMap<Integer, BigDecimal> getStepImmutableMap() {
        Map<Integer, BigDecimal> map = new HashMap<>(16);
        map.put(0, BigDecimal.ZERO);
        map.put(1, new BigDecimal("0.05"));
        map.put(2, new BigDecimal("0.1"));
        map.put(3, new BigDecimal("0.5"));
        map.put(4, BigDecimal.ONE);
        map.put(5, new BigDecimal("5"));
        map.put(6, BigDecimal.TEN);
        map.put(7, new BigDecimal("50"));
        map.put(-1, new BigDecimal("-0.05"));
        map.put(-2, new BigDecimal("-0.1"));
        map.put(-3, new BigDecimal("-0.5"));
        map.put(-4, new BigDecimal("-1"));
        map.put(-5, new BigDecimal("-5"));
        map.put(-6, new BigDecimal("-10"));
        map.put(-7, new BigDecimal("-50"));
        return ImmutableMap.copyOf(map);
    }

    private int getStepValueByMargin(BigDecimal margin) {
        if (margin == null) {
            return 0;
        }
        //不是很好的获取判定属于对应的区间位置
        if (margin.compareTo(stepImmutableMap.get(6)) >= 0) {
            return margin.compareTo(stepImmutableMap.get(7)) >= 0 ? 7 : 6;
        } else if (margin.compareTo(stepImmutableMap.get(4)) >= 0) {
            return margin.compareTo(stepImmutableMap.get(5)) >= 0 ? 5 : 4;
        } else if (margin.compareTo(stepImmutableMap.get(2)) >= 0) {
            return margin.compareTo(stepImmutableMap.get(3)) >= 0 ? 3 : 2;
        } else if (margin.compareTo(stepImmutableMap.get(0)) >= 0) {
            return margin.compareTo(stepImmutableMap.get(1)) >= 0 ? 1 : 0;
        } else if (margin.compareTo(stepImmutableMap.get(-6)) < 0) {
            return margin.compareTo(stepImmutableMap.get(-7)) <= 0 ? -7 : -6;
        } else if (margin.compareTo(stepImmutableMap.get(-4)) < 0) {
            return margin.compareTo(stepImmutableMap.get(-5)) <= 0 ? -5 : -4;
        } else if (margin.compareTo(stepImmutableMap.get(-2)) < 0) {
            return margin.compareTo(stepImmutableMap.get(-3)) <= 0 ? -3 : -2;
        } else {
            return margin.compareTo(stepImmutableMap.get(-1)) <= 0 ? -1 : 0;
        }
    }

    /**
     * 接收最新价
     *
     * @param brokerId
     * @param symbolId
     * @param price
     */
    private Boolean receiveTradePrice(Long brokerId, String symbolId, BigDecimal price) {
        String symbolKey = brokerId + UNDER_LINE + symbolId;
        ConcurrentSkipListMap<Long, BigDecimal> skipListMap = fifteenMinuteOpenPriceMap.get(symbolKey);
        if (skipListMap == null) {
            //表示两种情况一种非自选币对及固定币对；另一种是对应的币对没有完成初始化
            return true;
        }
        if (BigDecimal.ZERO.compareTo(price) >= 0) {
            //无效的价格
            return true;
        }
        //判断当前币对是否还在触发时间内
        long curTime = System.currentTimeMillis();
        long fifteenMinuteId = getCurrentFifteenMinuteId(curTime);
        if (!skipListMap.containsKey(fifteenMinuteId)) {
            skipListMap.putIfAbsent(fifteenMinuteId, price);
        }
        //判断当前币对是否还在触发时间内
        Long symbolTriggerTime = symbolTriggerLimitCache.getIfPresent(symbolKey);
        if (symbolTriggerTime != null && TEN_MINUTE_MILLIS + symbolTriggerTime > curTime) {
            return true;
        }
        RealtimeIntervalEnum realtimeInterval = orgRealtimeInterval.getOrDefault(brokerId, RealtimeIntervalEnum.H24);
        //计算24小时的涨幅
        long startTime = getRealtimeIntervalStartTime(curTime, realtimeInterval);
        BigDecimal openPrice = skipListMap.tailMap(startTime).firstEntry().getValue();
        BigDecimal margin;
        if (price.compareTo(openPrice) == 0) {
            return true;
        } else {
            //负数也是down -0.147 down = -0.14
            margin = price.subtract(openPrice).divide(openPrice, 2, RoundingMode.DOWN);
        }
        //对涨幅在+-5%之内的直接忽略
        if (margin.compareTo(up_ignore) < 0 && margin.compareTo(down_ignore) > 0) {
            return true;
        }
        int step = getStepValueByMargin(margin);
        String stepTriggerKey = symbolKey + UNDER_LINE + step;
        //获取当前step触发时间
        Long stepTriggerTime = stepTriggerLimitCache.getIfPresent(stepTriggerKey);
        if (stepTriggerTime != null && DAY_TIME_MILLIS + stepTriggerTime > curTime) {
            return true;
        }
        //本地缓存保证一个pod在10s内只有一个参与预触发竞争
        Long preSymbolTriggerTime = preSymbolTriggerMap.get(symbolKey);
        if (preSymbolTriggerTime != null && THIRTY_SECOND_MILLIS + preSymbolTriggerTime > curTime) {
            return true;
        } else if (preSymbolTriggerTime != null && !preSymbolTriggerMap.remove(symbolKey, preSymbolTriggerTime)) {
            return true;
        }
        if (preSymbolTriggerMap.putIfAbsent(symbolKey, curTime) != null) {
            return true;
        }
        log.info("pre symbol trigger push!brokerId:{},symbolId:{},step:{},margin:{},realtimeInterval:{}", brokerId, symbolId, step, margin, realtimeInterval.getInterval());
        //执行本地触发异步
        poolExecutor.execute(() -> handleTriggerPush(brokerId, symbolId, step, realtimeInterval));
        return true;
    }

    /**
     * 获取开始时间
     */
    private long getRealtimeIntervalStartTime(long curTime, RealtimeIntervalEnum realtimeInterval) {
        switch (realtimeInterval) {
            case D1:
                return curTime / DAY_TIME_MILLIS * DAY_TIME_MILLIS;
            case D1_8:
                return (new DateTime(curTime)).toDateTime(GMT).withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0).withHourOfDay(0).getMillis();
            default:
                return curTime - DAY_TIME_MILLIS;
        }
    }

    private void handleTriggerPush(Long brokerId, String symbolId, Integer step, RealtimeIntervalEnum realtimeInterval) {
        //抢占redis锁,用于本地缓存及redis缓存更新
        String lockKey = REALTIME_LOCK_PRE + brokerId + UNDER_LINE + symbolId;
        String randomStr = RandomStringUtils.randomAlphanumeric(8);
        if (redisStringLockUtil.lock(lockKey, 30, 1, 1000L, randomStr)) {
            try {
                //更新本地及redis缓存
                long curTime = System.currentTimeMillis();
                String symbolKey = brokerId + UNDER_LINE + symbolId;
                Long symbolTriggerTime = pushBusinessLimitRedisTemplate.opsForValue().get(symbolKey);
                if (symbolTriggerTime != null && TEN_MINUTE_MILLIS + symbolTriggerTime > curTime) {
                    //本地同步更新
                    symbolTriggerLimitCache.put(symbolKey, symbolTriggerTime);
                    log.info("Realtime push has been triggered in 10 minutes! {},{},{}", brokerId, symbolId, symbolTriggerTime);
                    return;
                }
                String stepTriggerKey = symbolKey + UNDER_LINE + step;
                Long stepTriggerTime = pushBusinessLimitRedisTemplate.opsForValue().get(stepTriggerKey);
                if (stepTriggerTime != null && DAY_TIME_MILLIS + stepTriggerTime > curTime) {
                    //本地同步更新
                    stepTriggerLimitCache.put(stepTriggerKey, stepTriggerTime);
                    log.info("Realtime step push has been triggered in 24 hours! {},{},{},{}", brokerId, symbolId, step, stepTriggerTime);
                    return;
                }
                //本地正式执行,获取行情realtime,存储执行记录,更新redis对应执行时间
                BigDecimal close;
                BigDecimal open;
                if (realtimeInterval.equals(RealtimeIntervalEnum.H24)) {
                    Realtime realtime = getCurrentRealtime(brokerId, symbolId);
                    if (realtime == null || new BigDecimal(realtime.getO()).compareTo(BigDecimal.ZERO) <= 0) {
                        return;
                    }
                    close = new BigDecimal(realtime.getC());
                    open = new BigDecimal(realtime.getO());
                } else {
                    KLine kline = getCurrentKline(brokerId, symbolId, realtimeInterval.getInterval());
                    if (kline == null || DecimalUtil.toBigDecimal(kline.getOpen()).compareTo(BigDecimal.ZERO) <= 0) {
                        return;
                    }
                    close = DecimalUtil.toBigDecimal(kline.getClose());
                    open = DecimalUtil.toBigDecimal(kline.getOpen());
                }
                BigDecimal differencePrice = close.subtract(open);
                //向下取整
                BigDecimal margin = differencePrice.divide(open, 4, RoundingMode.DOWN);
                int currentStep = getStepValueByMargin(margin);
                if (step != currentStep) {
                    //档位不一致的直接放弃
                    log.info("symbol trigger push discard! brokerId:{},symbolId:{},preStep:{},step:{}", brokerId, symbolId, step, currentStep);
                    return;
                }
                NoticeBusinessType businessType;
                if (differencePrice.compareTo(BigDecimal.ZERO) > 0) {
                    businessType = NoticeBusinessType.REALTIME_UP_NOTIFY;
                } else if (differencePrice.compareTo(BigDecimal.ZERO) < 0) {
                    businessType = NoticeBusinessType.REALTIME_DOWN_NOTIFY;
                } else {
                    return;
                }
                Symbol symbol = basicService.getOrgSymbol(brokerId, symbolId);
                if (symbol == null) {
                    return;
                }
                //存储记录,插入数据成功后,就更新redis，不管后续如何，都认为本次成功(重试不能保证实时性,所以不做重试)
                Map<String, String> reqParam = new HashMap<>(4);
                reqParam.put("symbol", symbol.getBaseTokenName() + "/" + symbol.getQuoteTokenName());
                reqParam.put("margin", margin.multiply(new BigDecimal(100)).stripTrailingZeros().toPlainString() + "%");
                reqParam.put("price", close.stripTrailingZeros().toPlainString());
                Map<String, String> urlParam = new HashMap<>(4);
                urlParam.put("symbol_id", symbolId);
                OrgBusinessPushRecordSimple recordSimple = new OrgBusinessPushRecordSimple(brokerId, businessType, reqParam, urlParam);
                Long pushRecordId;
                if (fixedSymbolSet.contains(symbolId)) {
                    pushRecordId = orgBusinessPushRecordService.addOrgBusinessPushRecord(recordSimple, 1);
                } else {
                    pushRecordId = orgBusinessPushRecordService.addOrgBusinessPushRecord(recordSimple, 0);
                }
                if (pushRecordId > 0) {
                    //更新本地缓存及redis缓存
                    log.info("symbol trigger push!brokerId:{},symbolId:{},step:{},margin:{},pushRecordId:{}", brokerId, symbolId, step, margin, pushRecordId);
                    pushBusinessLimitRedisTemplate.opsForValue().set(symbolKey, curTime, 10, TimeUnit.MINUTES);
                    pushBusinessLimitRedisTemplate.opsForValue().set(stepTriggerKey, curTime, 1, TimeUnit.DAYS);
                    symbolTriggerLimitCache.put(symbolKey, curTime);
                    stepTriggerLimitCache.put(stepTriggerKey, curTime);
                    if (fixedSymbolSet.contains(symbolId)) {
                        //给全部设备推送
                        appBusinessPushService.sendBrokerAllDeviceBusinessPushAsync(recordSimple, pushRecordId);
                    } else {
                        //给关注该自选的用户发送消息
                        List<Long> userIds = queryUserIdsByFavorite(brokerId, symbolId);
                        appBusinessPushService.sendBrokerBusinessPushAsync(recordSimple, userIds, pushRecordId);
                    }
                }
            } catch (Exception e) {
                log.error("realtime push error!{},{},{}", brokerId, symbolId, step, e);
            } finally {
                redisStringLockUtil.releaseLock(lockKey, randomStr);
            }
        } else {
            log.info("Another realtime push is being created! {},{},{}", brokerId, symbolId, step);
        }
    }

    private List<Long> queryUserIdsByFavorite(Long brokerId, String symbolId) {
        long startId = 0L;
        int limit = 5000;
        List<Long> userIds = new ArrayList<>();
        while (limit == 5000) {
            List<Long> curUserIds = favoriteMapper.queryUserIdsBySymbolId(brokerId, symbolId, startId, limit);
            limit = curUserIds.size();
            if (limit > 0) {
                startId = curUserIds.get(limit - 1);
                userIds.addAll(curUserIds);
            }
        }
        return userIds;
    }

    private long getCurrentFifteenMinuteId(long curTime) {
        return curTime / FIFTEEN_MINUTE_MILLIS * FIFTEEN_MINUTE_MILLIS;
    }

    /**
     * @author wangsc
     * @description 涨跌幅配置开始时间
     * @date 2020-08-08 15:34
     */
    public enum RealtimeIntervalEnum {
        /**
         * 24小时涨跌幅, UTC时间0点, UTC时间8点
         */
        H24("24h"),
        D1("1d"),
        D1_8("1d+8");
        private final String interval;

        RealtimeIntervalEnum(String interval) {
            this.interval = interval;
        }

        public static RealtimeIntervalEnum intervalOf(String interval) {
            RealtimeIntervalEnum[] enums = values();
            for (RealtimeIntervalEnum intervalEnum : enums) {
                if (intervalEnum.interval.equals(interval)) {
                    return intervalEnum;
                }
            }
            return H24;
        }

        public String getInterval() {
            return interval;
        }
    }
}
