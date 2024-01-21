package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.TextFormat;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.base.token.TokenCategory;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.GsonIgnore;
import io.bhex.broker.grpc.statistics.*;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsOdsService;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.OdsData;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.OdsMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.TokenMapper;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class OdsService {
    private static final long YEAR = 1000 * 60 * 60 * 24 * 365L;
    private static final long MONTH = 1000 * 60 * 60 * 24 * 30L;
    private static final long DAY = 1000 * 60 * 60 * 24L;
    private static final long HOUR = 1000 * 60 * 60L;
    private static final long MINUTE = 1000 * 60L;


    public static final int PAGE_SIZE = 10000;

    @Resource
    private OdsMapper odsMapper;
    @Resource
    private BrokerMapper brokerMapper;
    @Resource
    private StatisticsOdsService statisticsOdsService;
    @Resource
    private BaseBhBizConfigService baseBhBizConfigService;
    @Resource
    private BasicService basicService;
    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;
    @Resource
    private TokenMapper tokenMapper;

    @Resource
    private SymbolMapper symbolMapper;

    //@Scheduled(cron = "0 16/30 * * * ?")
//    public void init() {
//        BaseConfigInfo baseConfigInfo = baseBhBizConfigService.getOneConfig(0, "ods.data.swtich", "init.switch");
//        if (baseConfigInfo == null) {
//            return;
//        }
//        Map<String, Object> item = JsonUtil.defaultGson()
//                .fromJson(baseConfigInfo.getConfValue(), new TypeToken<Map>() {
//                }.getType());
//        boolean open = MapUtils.getBoolean(item, "open");
//        if (!open) {
//            return;
//        }
//        int start = MapUtils.getInteger(item, "start");
//        int end = MapUtils.getInteger(item, "end");
//
//
//        long now = System.currentTimeMillis();
//        for (int i = start; i >= end; i--) {
//            exec(new Date(now - i * DAY), "d");
//            generateTradeData(new Date(now - i * DAY), "d");
//        }
//    }

    //@Scheduled(cron = "0/30 * * * * ?")
    @Scheduled(cron = "0 10 1,2,9 * * ?")
    public void dayData() {
        exec(new Date(), "d");
        generateTradeData(new Date(), "d");
    }

    //@Scheduled(cron = "0/30 * * * * ?")
    @Scheduled(cron = "25 4,25 * * * ?")
    public void hourData() {
        exec(new Date(), "H");
        generateTradeData(new Date(), "H");
    }

    @Scheduled(cron = "25 12 1 * * ?")
    public void deleteUnusedHourData() {
        int row = odsMapper.deleteUnusedHourData(System.currentTimeMillis() - 2 * DAY);
        log.info("deleteUnusedHourData num = {}", row);
    }


    public void exec(Date nowDate, String unit) {
        List<BaseConfigInfo> configs = baseBhBizConfigService.getConfigs(0, "ods.data", null, null);
        if (CollectionUtils.isEmpty(configs)) {
            return;
        }
        Collections.shuffle(configs);
        for (BaseConfigInfo config : configs) {
            if (config.getConfKey().contains("converusdt") || config.getConfKey().contains("trade")) {
                continue;
            }

            String lockKey = BrokerLockKeys.ODS_TASK_LOCK_KEY + unit + config.getConfKey();
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.ODS_TASK_LOCK_KEY_EXPIRE);
            if (!lock) {
                continue;
            }
            try {
                long start = System.currentTimeMillis();
                ConfigModel configModel = parseConfigModel(config.getConfKey(), config.getConfValue(), unit);
                if (configModel.isClose()) {
                    log.warn("{} statistic was closed", config.getConfKey());
                    continue;
                }
                if (config.getConfKey().contains(".token")) {
                    generateTokenData(nowDate, configModel, config.getConfKey(), unit, true);
                } else if (config.getConfKey().contains(".symbol")) {
                    generateTokenData(nowDate, configModel, config.getConfKey(), unit, false);
                } else {
                    generateData(nowDate, configModel, config.getConfKey(), unit);
                }
                log.info("{} consume:{} ms", config.getConfKey(), System.currentTimeMillis() - start);
            } catch (Exception e) {
                log.info("generate data error", e);
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    private ConfigModel parseConfigModel(String configKey, String confValue, String unit) {
        ConfigModel configModel = JsonUtil.defaultGson()
                .fromJson(confValue, new TypeToken<ConfigModel>() {
                }.getType());
        if (unit.endsWith("d")) {
            configModel.setNewAdded(true);
            configModel.setTimeUnit("d");
            configModel.setTimeLength(0);
        } else if (unit.endsWith("H")) {
            configModel.setNewAdded(false);
            configModel.setTimeUnit("H");
            configModel.setTimeLength(24);
        }
        return configModel;
    }

    public void generateData(Date nowDate, ConfigModel configModel, String confKey, String unit) {
        String dataKey = confKey + "." + unit;
        List<Long> brokers = brokerMapper.selectAll().stream()
                .filter(b -> b.getStatus() == 1)
                .map(Broker::getOrgId)
                .collect(Collectors.toList());
        GroupSqlData sqlData = buildGroupDataSql(nowDate, configModel, dataKey, brokers);
        log.info("d1:{}", sqlData);
        String baseSql = sqlData.getSql();
        Map<Long, ValueDTO> groupMap = new HashMap<>();
        List<HashMap<String, Object>> allList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            String execSql = baseSql + " limit " + i * PAGE_SIZE + "," + PAGE_SIZE;
            log.info("exec sql:{}", execSql);
            List<HashMap<String, Object>> list;
            if (configModel.isStatisticData()) {
                list = statisticsOdsService.queryGroupDataList(configModel.getDb(), execSql, 0, confKey, unit, i * PAGE_SIZE, PAGE_SIZE);
            } else {
                list = odsMapper.queryGroupDataList(execSql);
            }
            allList.addAll(list);
            if (CollectionUtils.isEmpty(list) || list.size() < PAGE_SIZE) {
                break;
            }
        }
        log.info("generateData:{}", allList.size());
        if (CollectionUtils.isEmpty(allList)) {
            return;
        }
        for (HashMap<String, Object> data : allList) {
            long orgId = MapUtils.getLong(data, "orgId");
            ValueDTO tradeDTO = groupMap.getOrDefault(orgId, new ValueDTO());
            tradeDTO.setNumber(tradeDTO.getNumber() + 1);
            if (data.containsKey("amount")) {
                tradeDTO.setAmount(new BigDecimal(tradeDTO.getAmount())
                        .add(new BigDecimal(MapUtils.getString(data, "amount"))).stripTrailingZeros().toPlainString());
            }
            if (data.containsKey("quantity")) {
                tradeDTO.setQuantity(new BigDecimal(tradeDTO.getQuantity())
                        .add(new BigDecimal(MapUtils.getString(data, "quantity"))).stripTrailingZeros().toPlainString());
            }
            if (data.containsKey("user")) {
                tradeDTO.getUserIds().
                        add(MapUtils.getLong(data, "user"));
                tradeDTO.setUserCount(tradeDTO.getUserIds().size());
            }
            if (configModel.isConvertUSDT()) {
                String token = Strings.nullToEmpty(MapUtils.getString(data, "token"));
                BigDecimal usdt = converUsdt(orgId, token, new BigDecimal(MapUtils.getString(data, "quantity")));
                tradeDTO.setUsdt(new BigDecimal(tradeDTO.getUsdt()).add(usdt).stripTrailingZeros().toPlainString());
            }
            groupMap.put(orgId, tradeDTO);
        }
        for (long orgId : groupMap.keySet()) {
            if (odsMapper.countDataKeyByDateStr(orgId, dataKey, sqlData.getDateStr()) > 0) {
                continue;
            }
            OdsData odsData = OdsData.builder()
                    .created(System.currentTimeMillis())
                    .dataKey(dataKey)
                    .dataValue(JsonUtil.defaultGson().toJson(groupMap.get(orgId)))
                    .orgId(orgId)
                    .startTime(sqlData.getStart())
                    .endTime(sqlData.getEnd())
                    .dateStr(sqlData.getDateStr())
                    .token("")
                    .symbol("")
                    .build();
            log.info("d3:{}", odsData);
            odsMapper.insertSelective(odsData);
        }
    }

    public void generateTokenData(Date nowDate, ConfigModel configModel, String confKey, String unit, boolean isToken) {
        String dataKey = confKey + "." + unit;
        BaseConfigInfo excludeSymbolConfig = baseBhBizConfigService.getOneConfig(0L, "ods.data.switch", "exclude.symbols");
        List<String> excludeSymbols = excludeSymbolConfig != null
                ? Arrays.asList(excludeSymbolConfig.getConfValue().split(",")) : new ArrayList<>();
        Map<Long, Map<String, ValueDTO>> orgMap = new HashMap<>();
        List<Long> brokers = brokerMapper.selectAll().stream()
                .filter(b -> b.getStatus() == 1)
                .map(Broker::getOrgId)
                .collect(Collectors.toList());
        GroupSqlData sqlData = buildGroupDataSql(nowDate, configModel, dataKey, brokers);
        log.info("key:{} d1:{}", dataKey, sqlData);
        List<HashMap<String, Object>> allList = new ArrayList<>();
        String baseSql = sqlData.getSql();
        for (int i = 0; i < 1000; i++) {
            String execSql = baseSql + " limit " + i * PAGE_SIZE + "," + PAGE_SIZE;
            List<HashMap<String, Object>> list;
            if (configModel.isStatisticData()) {
                list = statisticsOdsService.queryGroupDataList(configModel.getDb(), execSql, 0, confKey, unit, i * PAGE_SIZE, PAGE_SIZE);
            } else {
                list = odsMapper.queryGroupDataList(execSql);
            }
            allList.addAll(list);
            if (CollectionUtils.isEmpty(list) || list.size() < PAGE_SIZE) {
                break;
            }
        }
        log.info("generateTokenData: {}", allList.size());
        if (CollectionUtils.isEmpty(allList)) {
            return;
        }
        for (HashMap<String, Object> data : allList) {
            long orgId = MapUtils.getLong(data, "orgId");
            String token = Strings.nullToEmpty(MapUtils.getString(data, "token"));
            if (isToken && excludeSymbols.contains(token)) {
                continue;
            }
            String symbol = Strings.nullToEmpty(MapUtils.getString(data, "symbol"));

            Map<String, ValueDTO> groupMap = orgMap.getOrDefault(orgId, new HashMap<>());

            for (String key : Arrays.asList(isToken ? token : symbol, "ALL")) {
                ValueDTO tradeDTO = groupMap.getOrDefault(key, new ValueDTO());
                tradeDTO.setNumber(tradeDTO.getNumber() + 1);
                if (data.containsKey("amount")) {
                    tradeDTO.setAmount(new BigDecimal(tradeDTO.getAmount())
                            .add(new BigDecimal(MapUtils.getString(data, "amount"))).stripTrailingZeros().toPlainString());
                }
                if (data.containsKey("quantity")) {
                    tradeDTO.setQuantity(new BigDecimal(tradeDTO.getQuantity())
                            .add(new BigDecimal(MapUtils.getString(data, "quantity"))).stripTrailingZeros().toPlainString());
                }
                if (data.containsKey("user")) {
                    tradeDTO.getUserIds().add(MapUtils.getLong(data, "user"));
                    tradeDTO.setUserCount(tradeDTO.getUserIds().size());
                }
                if (isToken && configModel.isConvertUSDT()) {
                    BigDecimal usdt = converUsdt(orgId, token, new BigDecimal(MapUtils.getString(data, "quantity")));
                    tradeDTO.setUsdt(new BigDecimal(tradeDTO.getUsdt()).add(usdt).stripTrailingZeros().toPlainString());
                }
                groupMap.put(key, tradeDTO);
            }
            orgMap.put(orgId, groupMap);
        }
        for (long orgId : orgMap.keySet()) {
            if (odsMapper.countDataKeyByDateStr(orgId, dataKey, sqlData.getDateStr()) > 0) {
                continue;
            }
            Map<String, ValueDTO> groupMap = orgMap.get(orgId);
            for (String key : orgMap.get(orgId).keySet()) {
                OdsData odsData = OdsData.builder()
                        .created(System.currentTimeMillis())
                        .dataKey(dataKey)
                        .dataValue(JsonUtil.defaultGson().toJson(groupMap.get(key)))
                        .orgId(orgId)
                        .startTime(sqlData.getStart())
                        .endTime(sqlData.getEnd())
                        .dateStr(sqlData.getDateStr())
                        .token(isToken ? key : "")
                        .symbol(isToken ? "" : key)
                        .build();
                log.info("key:{} d3:{}", dataKey, odsData);
                odsMapper.insertSelective(odsData);
            }

        }
    }

    private BigDecimal converUsdt(long orgId, String token, BigDecimal amount) {
        Rate rate = basicService.getV3Rate(orgId, token);
        if (rate == null) {
            log.info("token:{} no rate config", token);
            return BigDecimal.ZERO;
        }
        return amount.multiply(DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT")));
    }

    private GroupSqlData buildGroupDataSql(Date nowDate, ConfigModel configModel, String dataKey, List<Long> brokers) {
        long start = 0, end = 0;
        String formatStr = "yyyy";
        String unit = configModel.getTimeUnit();
        if (unit.equals("M")) {
            formatStr = formatStr + "-MM";
            end = getDateTime(formatStr, nowDate);

            start = getLastMonthFirstSecond(nowDate);
        } else if (unit.equals("d")) {
            formatStr = formatStr + "-MM-dd";
            end = getDateTime(formatStr, nowDate);
            start = end - DAY;
        } else if (unit.equals("H")) {
            formatStr = formatStr + "-MM-dd HH";
            end = getDateTime(formatStr, nowDate);
            start = end - HOUR;
        } else if (unit.equals("m")) {
            formatStr = formatStr + "-MM-dd HH:mm";
            end = getDateTime(formatStr, nowDate);
            start = end - MINUTE;
        }
        GroupSqlData sqlData = new GroupSqlData();
        sqlData.setStart(start);
        sqlData.setEnd(end);
        sqlData.setDateStr(getDateStr(formatStr, start));
        sqlData.setDateFormat(formatStr);
        String sql = configModel.getSql();
        if (sql != null && (!sql.toLowerCase().trim().startsWith("select") || sql.toLowerCase().contains("update ")
                || sql.toLowerCase().contains("insert ") || sql.toLowerCase().contains("delete ") || sql.contains(";"))) {
            throw new RuntimeException("error sql " + dataKey);
        }
        if (sql != null) {
            if ("long".equalsIgnoreCase(configModel.getCreatedType())) {
                sql = sql.replaceAll("#start#", start + "").replace("#end#", end + "");
            } else {
                sql = sql.replaceAll("#start#", "'" + getDateStr("yyyy-MM-dd HH:mm:ss", start) + "'")
                        .replaceAll("#end#", "'" + getDateStr("yyyy-MM-dd HH:mm:ss", end) + "'");
            }
            sql = sql.replaceAll("#orglist#", brokers.stream().map(Object::toString).collect(Collectors.joining(",")));
            sqlData.setSql(sql);
        }
        return sqlData;
    }

    @Data
    private static class TradeValueDTO {
        @SerializedName("bq")
        private String baseQuantity = "0";

        @SerializedName("qq")
        private String quoteQuantity = "0";

        @SerializedName("bu")
        private String baseUsdt = "0";
        @SerializedName("qu")
        private String quoteUsdt = "0";

        @SerializedName("uc")
        private Integer userCount = 0;

        @SerializedName("no")
        private Integer number = 0;

        @GsonIgnore
        private Set<Long> userIds = new HashSet<>();


        @SerializedName("sbq")
        private String sellBaseQuantity = "0";
        @SerializedName("sqq")
        private String sellQuoteQuantity = "0";
        @SerializedName("sbu")
        private String sellBaseUsdt = "0";
        @SerializedName("squ")
        private String sellQuoteUsdt = "0";
        @SerializedName("suc")
        private Integer sellUserCount = 0;
        @SerializedName("sno")
        private Integer sellNumber = 0;
        @GsonIgnore
        private Set<Long> sellUserIds = new HashSet<>();


        @SerializedName("bbq")
        private String buyBaseQuantity = "0";
        @SerializedName("bqq")
        private String buyQuoteQuantity = "0";
        @SerializedName("bbu")
        private String buyBaseUsdt = "0";
        @SerializedName("bqu")
        private String buyQuoteUsdt = "0";
        @SerializedName("buc")
        private Integer buyUserCount = 0;
        @SerializedName("bno")
        private Integer buyNumber = 0;
        @GsonIgnore
        private Set<Long> buyUserIds = new HashSet<>();
    }

    public void generateTradeData(Date nowDate, String timeUnit) {

        SwitchStatus switchStatus = baseBhBizConfigService.getConfigSwitchStatus(0L, "ods.data.switch", "close.trade.switch");
        if (switchStatus.isOpen()) {
            log.warn("trade statistic was closed");
            return;
        }

        BaseConfigInfo excludeSymbolConfig = baseBhBizConfigService.getOneConfig(0L, "ods.data.switch", "exclude.symbols");
        List<String> excludeSymbols = excludeSymbolConfig != null
                ? Arrays.asList(excludeSymbolConfig.getConfValue().split(",")) : new ArrayList<>();
        ConfigModel configModel = new ConfigModel();
        configModel.setTimeUnit(timeUnit);
        configModel.setCreatedType("date");
        configModel.setSql("select broker_id as orgId,symbol_id as symbolId,side,quantity baseQuantity,amount quoteQuantity,broker_user_id brokerUserId from ods_trade_detail where created_at between #start# and #end#");
        GroupSqlData sqlData = buildGroupDataSql(nowDate, configModel, "trade", new ArrayList<>());
        List<Integer> categories = Arrays.asList(
                TokenCategory.MAIN_CATEGORY.getNumber(),
                TokenCategory.OPTION_CATEGORY.getNumber(),
                TokenCategory.FUTURE_CATEGORY.getNumber()
        );
        String baseSql = sqlData.getSql();
        for (Integer category : categories) {
            String dataKey = "trade." + category + ".symbol." + timeUnit;
            String lockKey = BrokerLockKeys.ODS_TASK_LOCK_KEY + dataKey;
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.ODS_TASK_LOCK_KEY_EXPIRE);
            if (!lock) {
                continue;
            }
            try {
                long start = System.currentTimeMillis();
                List<Symbol> symbolList = symbolMapper.queryAll(Collections.singletonList(category));
                symbolList = symbolList.stream()
                        .filter(s -> !excludeSymbols.contains(s.getBaseTokenId()) && !excludeSymbols.contains(s.getQuoteTokenId()))
                        .collect(Collectors.toList());
                Map<Long, List<Symbol>> groupSymbol = symbolList.stream().collect(Collectors.groupingBy(Symbol::getOrgId));
                for (Long orgId : groupSymbol.keySet()) {
                    if (odsMapper.countDataKeyByDateStr(orgId, dataKey, sqlData.getDateStr()) > 0) {
                        continue;
                    }
                    Map<String, TradeValueDTO> symbolTradeMap = new HashMap<>();
                    String symbols = groupSymbol.get(orgId).stream().map(s -> "'" + s.getSymbolId() + "'").collect(Collectors.joining(","));
                    String orgSql = baseSql + " and broker_id = " + orgId + " and symbol_id in(" + symbols + ")";

                    for (int i = 0; i < 10000; i++) {
                        String execSql = orgSql + " limit " + i * PAGE_SIZE + "," + PAGE_SIZE;
                        log.info(execSql);
                        List<HashMap<String, Object>> list = statisticsOdsService.queryGroupTradeDataList(execSql, orgId, timeUnit, i * PAGE_SIZE, PAGE_SIZE, groupSymbol.get(orgId));
                        if (CollectionUtils.isEmpty(list)) {
                            break;
                        }
                        log.info("list:{}", list.size());
                        for (HashMap<String, Object> item : list) {
                            String symbolId = MapUtils.getString(item, "symbolId");


                            long user = MapUtils.getLong(item, "brokerUserId");
                            BigDecimal baseQuantity = new BigDecimal(MapUtils.getString(item, "baseQuantity"));
                            BigDecimal quoteQuantity = new BigDecimal(MapUtils.getString(item, "quoteQuantity"));
                            if (category == TokenCategory.FUTURE_CATEGORY.getNumber()) {
                                HashMap<String, Object> futureSymbol = statisticsOdsService.queryFutureSymbol(orgId, symbolId);
                                Boolean isReverse = MapUtils.getBoolean(futureSymbol, "isReverse");
                                BigDecimal contractMultiplier = BigDecimal.valueOf(MapUtils.getDoubleValue(futureSymbol, "contractMultiplier"));
                                if (isReverse) { //反向合约 trade_detail中 quantity*contractMultiplier
                                    quoteQuantity = baseQuantity.multiply(contractMultiplier).setScale(8, RoundingMode.DOWN);
                                } else { //正向合约 trade_detail中 amount*contractMultiplier
                                    quoteQuantity = quoteQuantity.multiply(contractMultiplier).setScale(8, RoundingMode.DOWN);
                                }
                            }


                            TradeValueDTO tradeDTO = symbolTradeMap.getOrDefault(symbolId, new TradeValueDTO());
                            TradeValueDTO allTradeDTO = symbolTradeMap.getOrDefault("ALL", new TradeValueDTO());
                            tradeDTO.setQuoteQuantity(new BigDecimal(tradeDTO.getQuoteQuantity()).add(quoteQuantity).stripTrailingZeros().toPlainString());
                            tradeDTO.setBaseQuantity(new BigDecimal(tradeDTO.getBaseQuantity()).add(baseQuantity).stripTrailingZeros().toPlainString());
                            tradeDTO.setNumber(tradeDTO.getNumber() + 1);
                            tradeDTO.getUserIds().add(user);
                            tradeDTO.setUserCount(tradeDTO.getUserIds().size());

                            allTradeDTO.setNumber(allTradeDTO.getNumber() + 1);
                            allTradeDTO.getUserIds().add(user);
                            allTradeDTO.setUserCount(allTradeDTO.getUserIds().size());

                            Symbol symbol = groupSymbol.get(orgId).stream().filter(s -> s.getSymbolId().equals(symbolId)).findFirst().orElse(null);
                            if (symbol == null) {
                                continue;
                            }
                            String baseToken = symbol.getBaseTokenId();
                            String quoteToken = symbol.getQuoteTokenId();

                            BigDecimal baseUsdt = converUsdt(orgId, baseToken, baseQuantity);
                            BigDecimal quoteUsdt;
                            if (category == TokenCategory.FUTURE_CATEGORY.getNumber()) {
                                quoteUsdt = quoteQuantity;
                            } else {
                                quoteUsdt = converUsdt(orgId, quoteToken, quoteQuantity);
                            }
                            tradeDTO.setBaseUsdt(new BigDecimal(tradeDTO.getBaseUsdt()).add(baseUsdt).stripTrailingZeros().toPlainString());
                            tradeDTO.setQuoteUsdt(new BigDecimal(tradeDTO.getQuoteUsdt()).add(quoteUsdt).stripTrailingZeros().toPlainString());

                            allTradeDTO.setBaseUsdt(new BigDecimal(allTradeDTO.getBaseUsdt()).add(baseUsdt).stripTrailingZeros().toPlainString());
                            allTradeDTO.setQuoteUsdt(new BigDecimal(allTradeDTO.getQuoteUsdt()).add(quoteUsdt).stripTrailingZeros().toPlainString());

                            if ("false".equals(MapUtils.getString(item, "side"))) {
                                tradeDTO.setBuyQuoteQuantity(new BigDecimal(tradeDTO.getBuyQuoteQuantity()).add(quoteQuantity).stripTrailingZeros().toPlainString());
                                tradeDTO.setBuyBaseQuantity(new BigDecimal(tradeDTO.getBuyBaseQuantity()).add(baseQuantity).stripTrailingZeros().toPlainString());

                                tradeDTO.setBuyNumber(tradeDTO.getBuyNumber() + 1);
                                tradeDTO.getBuyUserIds().add(user);
                                tradeDTO.setBuyUserCount(tradeDTO.getBuyUserIds().size());
                                tradeDTO.setBuyBaseUsdt(new BigDecimal(tradeDTO.getBuyBaseUsdt()).add(baseUsdt).stripTrailingZeros().toPlainString());
                                tradeDTO.setBuyQuoteUsdt(new BigDecimal(tradeDTO.getBuyQuoteUsdt()).add(quoteUsdt).stripTrailingZeros().toPlainString());

                                allTradeDTO.setBuyNumber(allTradeDTO.getBuyNumber() + 1);
                                allTradeDTO.getBuyUserIds().add(user);
                                allTradeDTO.setBuyUserCount(allTradeDTO.getBuyUserIds().size());
                                allTradeDTO.setBuyBaseUsdt(new BigDecimal(allTradeDTO.getBuyBaseUsdt()).add(baseUsdt).stripTrailingZeros().toPlainString());
                                allTradeDTO.setBuyQuoteUsdt(new BigDecimal(allTradeDTO.getBuyQuoteUsdt()).add(quoteUsdt).stripTrailingZeros().toPlainString());
                            } else {
                                tradeDTO.setSellQuoteQuantity(new BigDecimal(tradeDTO.getSellQuoteQuantity()).add(quoteQuantity).stripTrailingZeros().toPlainString());
                                tradeDTO.setSellBaseQuantity(new BigDecimal(tradeDTO.getSellBaseQuantity()).add(baseQuantity).stripTrailingZeros().toPlainString());
                                tradeDTO.setSellNumber(tradeDTO.getSellNumber() + 1);
                                tradeDTO.getSellUserIds().add(user);
                                tradeDTO.setSellUserCount(tradeDTO.getSellUserIds().size());
                                tradeDTO.setSellBaseUsdt(new BigDecimal(tradeDTO.getSellBaseUsdt()).add(baseUsdt).stripTrailingZeros().toPlainString());
                                tradeDTO.setSellQuoteUsdt(new BigDecimal(tradeDTO.getSellQuoteUsdt()).add(quoteUsdt).stripTrailingZeros().toPlainString());

                                allTradeDTO.setSellNumber(allTradeDTO.getSellNumber() + 1);
                                allTradeDTO.getSellUserIds().add(user);
                                allTradeDTO.setSellUserCount(allTradeDTO.getSellUserIds().size());
                                allTradeDTO.setSellBaseUsdt(new BigDecimal(allTradeDTO.getSellBaseUsdt()).add(baseUsdt).stripTrailingZeros().toPlainString());
                                allTradeDTO.setSellQuoteUsdt(new BigDecimal(allTradeDTO.getSellQuoteUsdt()).add(quoteUsdt).stripTrailingZeros().toPlainString());
                            }

                            symbolTradeMap.put(symbolId, tradeDTO);
                            symbolTradeMap.put("ALL", allTradeDTO);

                        }

                        if (CollectionUtils.isEmpty(list) || list.size() < PAGE_SIZE) {
                            break;
                        }
                    }

                    if (symbolTradeMap.size() == 0) {
                        continue;
                    }

                    for (String symbolId : symbolTradeMap.keySet()) {
                        OdsData odsData = OdsData.builder()
                                .created(System.currentTimeMillis())
                                .dataKey(dataKey)
                                .dataValue(JsonUtil.defaultGson().toJson(symbolTradeMap.get(symbolId)))
                                .orgId(orgId)
                                .startTime(sqlData.getStart())
                                .endTime(sqlData.getEnd())
                                .dateStr(sqlData.getDateStr())
                                .token("")
                                .symbol(symbolId)
                                .build();
                        log.info("insert trade data:{}", odsData);
                        odsMapper.insertSelective(odsData);

                        if ("ALL".equalsIgnoreCase(symbolId)) {
                            odsData = odsData.toBuilder()
                                    .dataKey("trade." + category + "." + timeUnit)
                                    .build();
                            odsMapper.insertSelective(odsData);
                        }
                    }
                }
                log.info("{} consume:{} ms", dataKey, System.currentTimeMillis() - start);
            } catch (Exception e) {
                log.info("generate data error", e);
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }

    }

    private String getDateStr(String formatStr, long time) {
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        return format.format(new Date(time));
    }

    private long getDateTime(String formatStr, Date date) {
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        String str = format.format(date);
        Date newDate = null;
        try {
            newDate = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return newDate != null ? newDate.getTime() : 0L;
    }

    private long getLastMonthFirstSecond(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);

        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, -1);
        return calendar.getTimeInMillis();
    }


    @Data
    private static class ValueDTO {
        @SerializedName("q")
        private String quantity = "0";

        @SerializedName("a")
        private String amount = "0"; //后币对数量

        @SerializedName("u")
        private String usdt = "0";

        @SerializedName("uc")
        private Integer userCount = 0;

        @SerializedName("no")
        private Integer number = 0;

        @GsonIgnore
        private Set<Long> userIds = new HashSet<>();
    }


    @Data
    private static class GroupSqlData {
        private long start;
        private long end;
        private String sql;
        private String dateStr;
        private String dateFormat;
    }

    @Data
    private static class ConfigModel {

        private boolean close;

        private boolean statisticData; //统计库数据

        private String db; //具体的统计库（目前有clear和默认库）

        private String sql;

        private int topNumber;

        private String createdType = "long";

        private String timeUnit; //M-月 d-天 h-时 m-分

        //private int number;

        private int timeLength; //只有类似取最近24小时写24起作用

        private boolean newAdded; //true 固定模式如每天一条记录 都是新增，false-动态模式 最近24小时的每1小时，就是24条记录每次都是覆盖

        private boolean convertUSDT;
    }

    public QueryOdsDataResponse queryOdsData(QueryOdsDataRequest request) {
        QueryOdsDataResponse.Builder builder = QueryOdsDataResponse.newBuilder();
        BaseConfigInfo groupConfigInfo = baseBhBizConfigService.getOneConfig(0L, "ods.data.group", request.getGroup());
        List<String> dataKeys = JsonUtil.defaultGson()
                .fromJson(groupConfigInfo.getConfValue(), new TypeToken<List<String>>() {
                }.getType());
        List<BaseConfigInfo> configs = baseBhBizConfigService.getConfigs(0, "ods.data", dataKeys, null);
        for (BaseConfigInfo dataKeyConfig : configs) {
            String dataKey = dataKeyConfig.getConfKey() + "." + request.getTimeUnit();
            ConfigModel configModel = parseConfigModel(dataKeyConfig.getConfKey(), dataKeyConfig.getConfValue(), request.getTimeUnit());
            int limit = request.getLimit();
            if ("H".equals(request.getTimeUnit())) {
                limit = 24;
            }
            List<OdsData> dataList = odsMapper.listDataKey(request.getOrgId(), dataKey, limit, request.getStartTime(), request.getEndTime());
            dataList = fullList(dataList, request.getStartTime(), request.getEndTime(), limit, configModel);
            List<io.bhex.broker.grpc.statistics.OdsData> resultList = dataList.stream().map(d -> {
                io.bhex.broker.grpc.statistics.OdsData.Builder b = io.bhex.broker.grpc.statistics.OdsData.newBuilder();
                b.setDateStr("H".equals(request.getTimeUnit()) ? d.getStartTime().toString() : d.getDateStr());
                b.setValue(d.getDataValue());
                return b.build();
            }).collect(Collectors.toList());

            OdsDataList odsDataList = OdsDataList.newBuilder().addAllOdsData(resultList).build();
            builder.putDataMap(dataKey, odsDataList);
        }
        //log.info("response:{}", builder.build());
        return builder.build();
    }

    //补齐中间的空洞
    private List<OdsData> fullList(List<OdsData> dataList, long reqStartTime, long reqEndTime, int limit, ConfigModel configModel) {
        if (CollectionUtils.isEmpty(dataList)) {
            dataList = new ArrayList<>();
        }
        List<OdsData> result = new ArrayList<>();
        List<Long> brokers = brokerMapper.selectAll().stream()
                .filter(b -> b.getStatus() == 1)
                .map(Broker::getOrgId)
                .collect(Collectors.toList());
        GroupSqlData sqlData = buildGroupDataSql(new Date(), configModel, "", brokers);
        long step = "M".equals(configModel.getTimeUnit()) ? MONTH : "d".equals(configModel.getTimeUnit()) ? DAY : "H".equals(configModel.getTimeUnit()) ? HOUR : Long.MAX_VALUE;
        long startTime = 0, endTime = 0;
        if (reqStartTime > 0 && reqEndTime > 0 && "d".equals(configModel.getTimeUnit())) {
            startTime = reqStartTime;
            endTime = reqEndTime;
        } else if ("H".equals(configModel.getTimeUnit())) {
            endTime = sqlData.getEnd() - 1000; //小时数据不包含当前时间
            startTime = sqlData.getEnd() - limit * step;
        }
        //补齐中间的空洞
        for (long dateMill = startTime; dateMill <= endTime; dateMill = dateMill + step) {
            String dateStr = getDateStr(sqlData.getDateFormat(), dateMill);
            Optional<OdsData> optional = dataList.stream().filter(d -> d.getDateStr().equals(dateStr)).findFirst();
            if (optional.isPresent()) {
                result.add(optional.get());
            } else {
                OdsData b = new OdsData();
                b.setDateStr("H".equals(configModel.getTimeUnit()) ? dateMill + "" : dateStr);
                b.setDataValue("{}");
                b.setStartTime(dateMill);
                result.add(b);
            }
        }
        return result;
    }

    public QueryOdsTokenDataResponse queryOdsTokenData(QueryOdsDataRequest request) {
        QueryOdsTokenDataResponse.Builder builder = QueryOdsTokenDataResponse.newBuilder();
        BaseConfigInfo groupConfigInfo = baseBhBizConfigService.getOneConfig(0L, "ods.data.group", request.getGroup());
        List<String> dataKeys = JsonUtil.defaultGson()
                .fromJson(groupConfigInfo.getConfValue(), new TypeToken<List<String>>() {
                }.getType());
        List<BaseConfigInfo> configs = baseBhBizConfigService.getConfigs(0, "ods.data", dataKeys, null);
        for (BaseConfigInfo dataKeyConfig : configs) {
            String dataKey = dataKeyConfig.getConfKey() + "." + request.getTimeUnit();

            long startTime = request.getStartTime();
            long endTime = request.getEndTime();
            int limit = request.getLimit();
            if (request.getTimeUnit().equals("H")) {
                limit = 24;
                startTime = 0;
                endTime = 0;
            }
            ConfigModel configModel = parseConfigModel(dataKeyConfig.getConfKey(), dataKeyConfig.getConfValue(), request.getTimeUnit());
            //           GroupSqlData sqlData = buildGroupDataSql(new Date(), configModel);


//            if (request.getStartTime() == 0) {
//                startTime = sqlData.getStart();
//            }
//            if (request.getStartTime() == 0) {
//                endTime = sqlData.getEnd();
//            }
            List<OdsData> dataList = odsMapper.listDataKey4Token(request.getOrgId(), dataKey, request.getToken(),
                    startTime, endTime);

            Map<String, List<OdsData>> tokenGroup = dataList.stream().collect(Collectors.groupingBy(OdsData::getToken));
            OdsTokenDataMap.Builder odsTokenDataMap = OdsTokenDataMap.newBuilder();

            for (String token : tokenGroup.keySet()) {
                List<OdsData> tokenDataList = fullList(tokenGroup.get(token), request.getStartTime(),
                        request.getEndTime(), limit, configModel);
                List<io.bhex.broker.grpc.statistics.OdsData> tokenResultList = tokenDataList.stream().map(d -> {
                    io.bhex.broker.grpc.statistics.OdsData.Builder b = io.bhex.broker.grpc.statistics.OdsData.newBuilder();
                    b.setDateStr("H".equals(request.getTimeUnit()) ? d.getStartTime().toString() : d.getDateStr());
                    b.setValue(d.getDataValue());
                    return b.build();
                }).collect(Collectors.toList());
                OdsDataList odsDataList = OdsDataList.newBuilder().addAllOdsData(tokenResultList).build();

                odsTokenDataMap.putTokenMap(token, odsDataList);
            }
            builder.putDataMap(dataKey, odsTokenDataMap.build());

        }
        //log.info("response:{}", builder.build());
        return builder.build();
    }

    public QueryOdsSymbolDataResponse queryOdsSymbolData(QueryOdsDataRequest request) {
        QueryOdsSymbolDataResponse.Builder builder = QueryOdsSymbolDataResponse.newBuilder();
        BaseConfigInfo groupConfigInfo = baseBhBizConfigService.getOneConfig(0L, "ods.data.group", request.getGroup());
        List<String> dataKeys = JsonUtil.defaultGson()
                .fromJson(groupConfigInfo.getConfValue(), new TypeToken<List<String>>() {
                }.getType());
        List<BaseConfigInfo> configs = baseBhBizConfigService.getConfigs(0, "ods.data", dataKeys, null);
        for (BaseConfigInfo dataKeyConfig : configs) {
            String dataKey = dataKeyConfig.getConfKey() + "." + request.getTimeUnit();
            int limit = request.getLimit();
            long startTime = request.getStartTime();
            long endTime = request.getEndTime();
            if ("H".equals(request.getTimeUnit())) {
                limit = 24;
                startTime = 0;
                endTime = 0;
            }

            ConfigModel configModel = parseConfigModel(dataKeyConfig.getConfKey(), dataKeyConfig.getConfValue(), request.getTimeUnit());
            //           GroupSqlData sqlData = buildGroupDataSql(new Date(), configModel);

//            if (request.getStartTime() == 0) {
//                startTime = sqlData.getStart();
//            }
//
//            if (request.getStartTime() == 0) {
//                endTime = sqlData.getEnd();
//            }

            List<OdsData> dataList = odsMapper.listDataKey4Symbol(request.getOrgId(), dataKey, request.getSymbol(),
                    startTime, endTime);

            Map<String, List<OdsData>> symbolGroup = dataList.stream().collect(Collectors.groupingBy(OdsData::getSymbol));
            OdsSymbolDataMap.Builder odsSymbolDataMap = OdsSymbolDataMap.newBuilder();

            for (String symbol : symbolGroup.keySet()) {
                List<OdsData> symbolDataList = fullList(symbolGroup.get(symbol), request.getStartTime(),
                        request.getEndTime(), limit, configModel);
                List<io.bhex.broker.grpc.statistics.OdsData> symbolResultList = symbolDataList.stream().map(d -> {
                    io.bhex.broker.grpc.statistics.OdsData.Builder b = io.bhex.broker.grpc.statistics.OdsData.newBuilder();
                    b.setDateStr(d.getDateStr());
                    b.setDateStr("H".equals(request.getTimeUnit()) ? d.getStartTime().toString() : d.getDateStr());
                    b.setValue(d.getDataValue());
                    return b.build();
                }).collect(Collectors.toList());
                OdsDataList odsDataList = OdsDataList.newBuilder().addAllOdsData(symbolResultList).build();

                odsSymbolDataMap.putSymbolMap(symbol, odsDataList);
            }
            builder.putDataMap(dataKey, odsSymbolDataMap.build());

        }
        // log.info("response:{}", builder.build());
        return builder.build();
    }

    public QueryTopDataResponse queryTopData(QueryTopDataRequest request) {

        Map<String, Object> item = new HashMap<>();
        item.put("brokerId", request.getOrgId());
        item.put("startDate", request.getStartDate());
        item.put("endDate", request.getEndDate());
        item.put("start", request.getLastIndex());
        item.put("pageSize", request.getLimit());
        item.put("tradeType", request.getTradeType());

        if (!StringUtils.isEmpty(request.getToken())) {
            Token token = tokenMapper.getToken(request.getOrgId(), request.getToken());
            if (token == null) {
                return QueryTopDataResponse.newBuilder().build();
            }
            item.put("tokenId", request.getToken());
            if ("tradeFeeTopContractToken".equals(request.getBizKey())) {
                List<Symbol> localSymbolList = symbolMapper.queryOrgSymbols(request.getOrgId(), Collections.singletonList(TokenCategory.FUTURE_CATEGORY_VALUE));
                if (CollectionUtils.isEmpty(localSymbolList)) {
                    return QueryTopDataResponse.newBuilder().build();
                }
                List<String> symbols = localSymbolList.stream()
                        .filter(s -> s.getQuoteTokenId().equals(request.getToken()))
                        .map(Symbol::getSymbolId)
                        .collect(Collectors.toList());
                if (CollectionUtils.isEmpty(symbols)) {
                    return QueryTopDataResponse.newBuilder().build();
                }
                item.put("symbols", symbols);
                item.put("tradeType", 3);
            }
        }
        if (!StringUtils.isEmpty(request.getSymbol())) {
            if (!"tradeFeeTopOtcToken".equals(request.getBizKey())) { //otc也用symbolid来做查询的
                Symbol symbol = symbolMapper.getOrgSymbol(request.getOrgId(), request.getSymbol());
                if (symbol == null && request.getTradeType() < 100) {
                    return QueryTopDataResponse.newBuilder().build();
                }
            }
            item.put("symbolId", request.getSymbol());
        }


        List<HashMap<String, Object>> list = new ArrayList<>();
        try {
            list = statisticsOdsService.queryGroupDataListByBizKey(request.getBizKey(), item);
        } catch (Exception e) {
            log.error("error key:{}", TextFormat.shortDebugString(request), e);
        }

        if (CollectionUtils.isEmpty(list)) {
            return QueryTopDataResponse.newBuilder().build();
        }
        int index = request.getLastIndex();
        List<TopData> topDataList = new ArrayList<>();
        for (HashMap<String, Object> i : list) {
            TopData.Builder topData = TopData.newBuilder();
            i.forEach((k, v) -> topData.putItem(k, v != null ? v.toString() : ""));
            topData.putItem("index", (++index) + "");
            topDataList.add(topData.build());
        }
        return QueryTopDataResponse.newBuilder().addAllData(topDataList).build();
    }
}
