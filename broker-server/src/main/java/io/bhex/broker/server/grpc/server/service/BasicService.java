/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/8/21
 *@Author peiwei.ren@bhex.io
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.bhex.base.admin.ContractDetail;
import io.bhex.base.admin.ListAllContractByOrgIdsRequest;
import io.bhex.base.admin.ListContractReply;
import io.bhex.base.admin.OrgType;
import io.bhex.base.margin.*;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.UnderlyingTypeEnum;
import io.bhex.base.quote.*;
import io.bhex.base.token.GetEtfSymbolPriceRequest;
import io.bhex.base.token.GetTokenRequest;
import io.bhex.base.token.GetUnderlyingRequest;
import io.bhex.base.token.TokenFuturesInfo;
import io.bhex.base.token.TokenOptionInfo;
import io.bhex.base.token.*;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.ExtraConfigUtil;
import io.bhex.broker.common.util.ExtraTagUtil;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.basic.KycLevel;
import io.bhex.broker.grpc.basic.KycLevelConfig;
import io.bhex.broker.grpc.basic.Underlying;
import io.bhex.broker.grpc.basic.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.margin.RiskConfig;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.domain.FuturesRiskLimitWhiteList;
import io.bhex.broker.server.grpc.client.service.*;
import io.bhex.broker.server.model.BrokerKycConfig;
import io.bhex.broker.server.model.Country;
import io.bhex.broker.server.model.QuoteToken;
import io.bhex.broker.server.model.SensitiveWords;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.*;
import io.bhex.ex.otc.AllBrokerExtResponse;
import io.bhex.ex.otc.BrokerExt;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BasicService {

    @Resource
    private TokenMapper tokenMapper;

    @Resource
    private SymbolMapper symbolMapper;

    @Resource
    private CountryMapper countryMapper;

    @Resource
    private GrpcTokenService grpcTokenService;

    @Resource
    private GrpcSymbolService grpcSymbolService;

    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private SensitiveWordsMapper sensitiveWordsMapper;

    @Resource
    private KycLevelMapper kycLevelMapper;

    @Resource
    private KycLevelConfigMapper kycLevelConfigMapper;

    @Resource
    private CardTypeMapper cardTypeMapper;

    @Resource
    private CardTypeLocaleMapper cardTypeLocaleMapper;

    @Resource
    private GrpcOrgContractService orgContractService;

    @Resource
    private GrpcQuoteService grpcQuoteService;

    @Resource
    private FuturesBasicService futuresBasicService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private OrderSourceMapper orderSourceMapper;

    // do not delete!!!
    @Resource
    private BrokerService brokerService;

    @Resource
    private BrokerExchangeMapper brokerExchangeMapper;

    @Resource
    private GrpcMarginService grpcMarginService;

    @Resource
    private BrokerKycConfigMapper brokerKycConfigMapper;
    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private GrpcOtcService grpcOtcService;
    @Resource
    private BaseBizConfigService baseBizConfigService;

    private static final String COMMON_INI_KEY_RISKLIMIT_WHITELIST = "futures_risklimit_whitelist";
    private static final String FILTER_FUTURES_RISKLIMIT = "filter_futures_risklimit";

    public static final String[] PLATFORM_LEFT_PADDING0 = new String[]{"", "0", "00", "000", "0000", "00000"};

    //common basic info
    private static ImmutableList<Long> countryIdList = ImmutableList.of();
    private static ImmutableMap<Long, String> countryCodeMap = ImmutableMap.of();
    private static ImmutableMap<String, Country> countryDomainShortNameMap = ImmutableMap.of();
    private static ImmutableList<String> nationalCodeList = ImmutableList.of();
    private static ImmutableTable<Long, String, String> countryNameTable = ImmutableTable.of();
//    private static final Cache<String, FXRate> FX_RATE_CACHE = CacheBuilder.newBuilder()
//            .expireAfterWrite(60, TimeUnit.SECONDS).build();

    private static final Cache<String, Rate> TOKEN_RATE_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(60, TimeUnit.SECONDS).build();

    //coin basic info
    private List<Integer> coinCategories = Arrays.asList(
            TokenCategory.MAIN_CATEGORY_VALUE,
            TokenCategory.INNOVATION_CATEGORY_VALUE,
            TokenCategory.AGGREGATE_CATEGORY_VALUE
    );
    private static ImmutableMap<String, String> tokenNameMap = ImmutableMap.of();
    private static ImmutableMap<String, String> symbolNameMap = ImmutableMap.of();
    private static ImmutableMap<Long, List<Token>> orgTokenMap = ImmutableMap.of();
    private static ImmutableMap<String, Token> orgTokenDetailMap = ImmutableMap.of();

    private static ImmutableMap<String, io.bhex.broker.grpc.basic.Symbol> orgSymbolMap = ImmutableMap.of();

    //option basic info
    private List<Integer> optionCategories = Collections.singletonList(TokenCategory.OPTION_CATEGORY.getNumber());
    private static ImmutableMap<String, SymbolDetail> exchangeSymbolDetailOptionMap = ImmutableMap.of();
    private ImmutableMap<String, TokenDetail> tokenDetailOptionMap = ImmutableMap.of();
    private ImmutableMap<String, Symbol> tokenSymbolOptionMap = ImmutableMap.of();

    //futures basic info
    public static ImmutableMap<String, SymbolDetail> exchangeSymbolDetailFuturesMap = ImmutableMap.of();
    public static ImmutableMap<String, Long> orgExchangeFuturesMap = ImmutableMap.of();
    public static ImmutableMap<String, Underlying> underlyingMap = ImmutableMap.of();
    public static ImmutableMap<String, TokenFuturesInfo> tokenFuturesInfoMap = ImmutableMap.of();
    public static ImmutableMap<String, List<SymbolDetail>> futureSymbolByQuoteTokenIdMap = ImmutableMap.of();
    public static ImmutableMap<String, Symbol> brokerOrgFuturesSymbolMap = ImmutableMap.of();

    public static ImmutableMap<String, MakerBonusConfig> makerBonusConfigImmutableMap = ImmutableMap.of();

    private static ImmutableMap<String, BrokerKycConfig> brokerKycConfigMap = ImmutableMap.of();
    private static ImmutableMap<Integer, KycLevel> kycLevelMap = ImmutableMap.of();

    private static ImmutableTable<Integer, String, String> cardTypeTable = ImmutableTable.of();


    private static ImmutableMap<String, TokenConfig> orgMarginTokenMap = ImmutableMap.of();

    private static ImmutableMap<String, OrderSource> orderSourceMap = ImmutableMap.of();

    public static ImmutableMap<Long, BrokerExt> brokerExtMap = ImmutableMap.of();

    private static final Cache<Long, List<FuturesRiskLimitWhiteList>> ORG_FUTURES_RISKLIMIT_WHITELIST_CACHE =
            CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

    private static ImmutableMap<Long, List<RiskConfig>> orgMarginRiskConfig = ImmutableMap.of();

    public static Set<Long> marginOrgIds = ConcurrentHashMap.newKeySet();

    public static ImmutableMap<Long, List<InterestConfig>> orgMarginInterestConfig = ImmutableMap.of();

    private static final Cache<String, BigDecimal> TOKEN_INDICES_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS).build();

    @PostConstruct
    public void init() throws Exception {
        try {
            List<Country> countryList = countryMapper.queryAllCountry();
            countryIdList = ImmutableList.copyOf(countryList.stream().map(Country::getId).collect(Collectors.toList()));
            countryCodeMap = ImmutableMap.copyOf(countryList.stream().collect(Collectors.toMap(Country::getId, Country::getDomainShortName)));
            countryDomainShortNameMap = ImmutableMap.copyOf(countryList.stream().collect(Collectors.toMap(Country::getDomainShortName, country -> country)));
            nationalCodeList = ImmutableList.copyOf(countryList.stream().map(Country::getNationalCode).collect(Collectors.toList()));

            List<CountryDetail> countryDetailList = countryMapper.queryAllCountryDetail();
            Table<Long, String, String> table = HashBasedTable.create();
            countryDetailList.forEach(country -> table.put(country.getCountryId(), country.getLanguage(), country.getCountryName()));
            countryNameTable = ImmutableTable.copyOf(table);
            scheduleTask();
            initMarginConfig();
            refreshOrderSource();
            scheduleMakerBonusRate();

            try {
                AllBrokerExtResponse brokerExtResponse = grpcOtcService.getAllBrokerExt();
                if (brokerExtResponse != null && brokerExtResponse.getBrokerExtCount() > 0) {
                    brokerExtMap = ImmutableMap.copyOf(brokerExtResponse.getBrokerExtList().stream().collect(Collectors.toMap(BrokerExt::getBrokerId, brokerExt -> brokerExt)));
                }
            } catch (Exception ex) {
                log.info("brokerExtMap error {}", ex);
            }
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    //下架掉交易所已经下架的币对
    @Scheduled(cron = "0,15,30,45 * * * * ?")
    public void syncExchangeCoinSymbolStatus() {
        String lockKey = "syncExchangeCoinSymbolStatus";
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, 60_000);
        if (!lock) {
            return;
        }
        try {
            List<Broker> brokerList = brokerMapper.queryAvailableBroker();
            for (Broker broker : brokerList) {
                List<Symbol> symbols = symbolMapper.queryOrgSymbols(broker.getOrgId(), coinCategories);
                List<Long> exchangeIds = symbols.stream()
                        .map(Symbol::getExchangeId)
                        .distinct()
                        .collect(Collectors.toList());
                int pageSize = 1000;
                for (Long exchangeId : exchangeIds) {
                    for (int i = 1; i < 10; i++) {
                        GetExchangeSymbolsRequest request = GetExchangeSymbolsRequest.newBuilder()
                                .setBaseRequest(BaseReqUtil.getBaseRequest(broker.getOrgId()))
                                .setExchangeId(exchangeId).setCurrent(i).setPageSize(pageSize)
                                .build();
                        List<ExchangeSymbolDetail> details = grpcSymbolService.getExchangeSymbols(request);
                        if (CollectionUtils.isEmpty(details)) {
                            break;
                        }
                        for (ExchangeSymbolDetail detail : details) {
                            if (!detail.getPublished()) { //交易所下线币对 券商对应币对自动下线
                                boolean published = symbols.stream()
                                        .anyMatch(s -> s.getExchangeId().equals(exchangeId) && detail.getSymbolId().equals(s.getSymbolId()) && s.getStatus() == 1);
                                if (published) {
                                    int rows = symbolMapper.disableByExchangeId(exchangeId, detail.getSymbolId());
                                    log.info("disable symbol exchange:{} symbol:{} rows:{}", exchangeId, detail.getSymbolId(), rows);
                                }
                            }
                        }
                        if (details.size() < pageSize) {
                            break;
                        }
                    }
                }
            }
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }
    }

    @Scheduled(cron = "0/5 * * * * ?")
    public void scheduleTask() throws Exception {
        try {
            List<Broker> brokerList = brokerMapper.queryAvailableBroker();
            {
                List<Token> tokenList = Lists.newArrayList();
                for (Broker broker : brokerList) {
                    tokenList.addAll(tokenMapper.queryAllByOrgId(coinCategories, broker.getOrgId()));
                }
                tokenNameMap = ImmutableMap.copyOf(tokenList.stream().collect(Collectors.toMap(token -> String.format("%s_%s", token.getOrgId(), token.getTokenId()), Token::getTokenName, (p, q) -> p)));
                orgTokenMap = ImmutableMap.copyOf(tokenList.stream().collect(Collectors.groupingBy(Token::getOrgId)));
                orgTokenDetailMap = ImmutableMap.copyOf(tokenList.stream().collect(Collectors.toMap(token -> String.format("%s_%s", token.getOrgId(), token.getTokenId()), token -> token, (p, q) -> p)));
            }
            {
                List<Symbol> symbolList = Lists.newArrayList();
                List<io.bhex.broker.grpc.basic.Symbol> grpcSymbolList = Lists.newArrayList();
                for (Broker broker : brokerList) {
                    symbolList.addAll(symbolMapper.queryOrgSymbols(broker.getOrgId(), coinCategories));
                    List<io.bhex.broker.grpc.basic.Symbol> orgSymbols = querySymbolList(broker.getOrgId(), coinCategories);
//                    //特殊处理，此处list只传一个
//                    GetMakerBonusConfigReply reply = grpcSymbolService.querySymbolMakerBonusConfig(GetMakerBonusConfigRequest.newBuilder()
//                            .addBrokerId(broker.getOrgId()).build(), broker.getOrgId());
//                    List<MakerBonusConfig> makerBonusConfigLists = reply.getBonusMapMap().get(broker.getOrgId()).getConfigsList();
//                    Map<String, MakerBonusConfig> makerBonusConfigMap = makerBonusConfigLists.stream()
//                            .collect(Collectors.toMap(config -> Strings.nullToEmpty(config.getSymbolId()).trim(), item -> item, (p, q) -> q));
//                    for (io.bhex.broker.grpc.basic.Symbol orgSymbol : orgSymbols) {
//                        if (makerBonusConfigMap.get(orgSymbol.getSymbolId()) != null) {
//                            orgSymbol = orgSymbol.toBuilder().setMakerBonusRate(makerBonusConfigMap.get(orgSymbol.getSymbolId()).getMakerBonusRate())
//                                    .setMinInterestFeeRate(makerBonusConfigMap.get(orgSymbol.getSymbolId()).getMinInterestFeeRate())
//                                    .setMinTakerFeeRate(makerBonusConfigMap.get(orgSymbol.getSymbolId()).getMinTakerFeeRate())
//                                    .build();
//                        } else {
//                            orgSymbol = orgSymbol.toBuilder().setMakerBonusRate("0").setMinInterestFeeRate("0").setMinTakerFeeRate("0").build();
//                        }
//                        grpcSymbolList.add(orgSymbol);
//                    }
                    grpcSymbolList.addAll(orgSymbols);
                }
                symbolNameMap = ImmutableMap.copyOf(symbolList.stream().collect(Collectors.toMap(symbol -> String.format("%s_%s", symbol.getOrgId(), symbol.getSymbolId()), Symbol::getSymbolName, (p, q) -> p)));
                Map<String, io.bhex.broker.grpc.basic.Symbol> tmpOrgSymbolMap = Maps.newHashMap();
                if (!CollectionUtils.isEmpty(grpcSymbolList)) {
                    grpcSymbolList.forEach(symbol -> {
                        tmpOrgSymbolMap.put(symbol.getOrgId() + "_" + symbol.getSymbolId(), symbol);
                        if (!symbol.getSymbolId().equalsIgnoreCase(symbol.getSymbolName())) {
                            tmpOrgSymbolMap.put(symbol.getOrgId() + "_" + symbol.getSymbolName(), symbol);
                        }
                    });
                }
                BasicService.orgSymbolMap = ImmutableMap.copyOf(tmpOrgSymbolMap);
            }

            //option
//            {
//                Map<String, TokenDetail> tmpTokenDetailMap = Maps.newHashMap();
//                for (Broker broker : brokerList) {
//                    if (!BrokerService.checkModule(broker.getOrgId(), FunctionModule.OPTION)) {
//                        continue;
//                    }
//                    tmpTokenDetailMap.putAll(queryTokenDetailOptionMap(tokenMapper.queryAllByOrgId(optionCategories, broker.getOrgId()), broker.getOrgId()));
//                }
//                tokenDetailOptionMap = ImmutableMap.copyOf(tmpTokenDetailMap);
//            }
//            {
//
//                Map<String, Symbol> tmpTokenSymbolOptionMap = Maps.newHashMap();
//                List<Symbol> symbolList = Lists.newArrayList();
//                List<SymbolDetail> symbolDetailList = Lists.newArrayList();
//                for (Broker broker : brokerList) {
//                    if (!BrokerService.checkModule(broker.getOrgId(), FunctionModule.OPTION)) {
//                        continue;
//                    }
//                    List<Symbol> localSymbolList = symbolMapper.queryOrgSymbols(broker.getOrgId(), optionCategories);
//                    tmpTokenSymbolOptionMap.putAll(queryTokenSymbolOptionMap(localSymbolList));
//                    symbolList.addAll(localSymbolList);
//                    symbolDetailList.addAll(querySymbolDetailList(broker.getOrgId(), optionCategories));
//                }
//                tokenSymbolOptionMap = ImmutableMap.copyOf(tmpTokenSymbolOptionMap);
//
//                Map<String, SymbolDetail> symbolDetailMap = symbolDetailList.stream()
//                        .collect(Collectors.toMap(symbol -> String.format("%s_%s", symbol.getExchangeId(), symbol.getSymbolId()), symbol -> symbol, (p, q) -> p));
//                Map<String, SymbolDetail> tmpSymbolMap = Maps.newHashMap();
//                if (!CollectionUtils.isEmpty(symbolList)) {
//                    symbolList.forEach(symbol -> {
//                        String key = symbol.getExchangeId() + "_" + symbol.getSymbolId();
//                        if (symbolDetailMap.containsKey(key)) {
//                            tmpSymbolMap.put(key, symbolDetailMap.get(key));
//                        }
//                    });
//                }
//                BasicService.exchangeSymbolDetailOptionMap = ImmutableMap.copyOf(tmpSymbolMap);
//            }

            //futures
            Long orgId = 0L;
            futuresBasicService.initFuturesSymbols(orgId);
            futuresBasicService.initFuturesUnderlying(orgId);
            futuresBasicService.initFuturesTokens(orgId);

            refreshKycLevelMap();
            brokerKycConfigMap = ImmutableMap.copyOf(brokerKycConfigMapper.selectAll()
                    .stream()
                    .filter(k -> k.getStatus().equals(1))
                    .collect(Collectors.toMap(k -> String.format("%s_%s", k.getOrgId(), k.getCountryId()), k -> k)));
            refreshCardTypeTable();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }


    @Scheduled(cron = "0/30 * * * * ?")
    public void initMarginConfig() throws Exception {
        try {
            long orgId = 0L;
            GetTokenConfigRequest request = GetTokenConfigRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build()).build();
            GetTokenConfigReply reply = grpcMarginService.getToken(request);
            List<TokenConfig> list = reply.getTokenConfigList();
            if (list != null) {
                Map<String, TokenConfig> map = list.stream().collect(Collectors.toMap(token -> token.getOrgId() + "_" + token.getTokenId(), token -> token, (p, q) -> p));
                marginOrgIds = list.stream().map(TokenConfig::getOrgId).collect(Collectors.toSet());
                orgMarginTokenMap = ImmutableMap.copyOf(map);
            }
            Map<Long, List<RiskConfig>> riskMap = new HashMap<>();
            marginOrgIds.forEach(id -> {
                try {
                    List<RiskConfig> riskConfigs = new ArrayList<>();
                    GetRiskConfigRequest getRiskConfigRequest = GetRiskConfigRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(id).build()).build();
                    GetRiskConfigReply result = grpcMarginService.getRisk(getRiskConfigRequest);
                    if (result.getRet() == 0) {
                        io.bhex.base.margin.RiskConfig config = result.getRiskConfig();
                        RiskConfig risk = RiskConfig.newBuilder()
                                .setOrgId(config.getOrgId())
                                .setWithdrawLine(DecimalUtil.toTrimString(config.getWithdrawLine()))
                                .setWarnLine(DecimalUtil.toTrimString(config.getWarnLine()))
                                .setAppendLine(DecimalUtil.toTrimString(config.getAppendLine()))
                                .setStopLine(DecimalUtil.toTrimString(config.getStopLine()))
                                .setMaxLoanLimiit(DecimalUtil.toTrimString(config.getMaxLoanLimit()))
                                .setNotifyType(config.getNotifyType())
                                .setNotifyNumber(config.getNotifyNumber())
                                .build();
                        riskConfigs.add(risk);
                    }
                    riskMap.put(id, riskConfigs);
                } catch (Exception e) {
                    log.warn("init risk error", e);
                }

            });
            orgMarginRiskConfig = ImmutableMap.copyOf(riskMap);
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    @Scheduled(cron = "0/30 * * * * ?")
    public void initMarginInterestConfig() {
        List<Broker> brokerList = brokerMapper.queryAvailableBroker();
        List<InterestConfig> interestConfigs = new ArrayList<>();
        for (Broker broker : brokerList) {
            try {
                QueryInterestByLevelRequest request = QueryInterestByLevelRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(broker.getOrgId()).build())
                        .setLevelConfigId(-1L)
                        .build();
                QueryInterestByLevelReply reply = grpcMarginService.queryInterestByLevel(request);
                interestConfigs.addAll(reply.getInterestConfigList());
            } catch (Exception e) {
                log.error("init margin interest error", e.getMessage(), e);
            }
        }
        Map<Long, List<InterestConfig>> map = interestConfigs.stream().collect(Collectors.groupingBy(InterestConfig::getOrgId));
        orgMarginInterestConfig = ImmutableMap.copyOf(map);
    }

    public List<InterestConfig> getMarginInterestByLevels(Long orgId, String tokenId, List<Long> levelIds) {
        return Optional.ofNullable(orgMarginInterestConfig.get(orgId))
                .orElse(Collections.emptyList()).stream()
                .filter(interestConfig -> tokenId.equals("") || interestConfig.getTokenId().equals(tokenId))
                .filter(interestConfig -> levelIds.isEmpty() || levelIds.contains(interestConfig.getLevelConfigId()))
                .collect(Collectors.toList());
    }

    @Scheduled(cron = "0 0/5 * * * ?")
    public void scheduleMakerBonusRate() throws Exception {
        try {
            List<Broker> brokerList = brokerMapper.queryAvailableBroker();
            Map<String, MakerBonusConfig> tmpMakerBonusRateConfigMap = Maps.newHashMap();
            for (Broker broker : brokerList) {
                GetMakerBonusConfigReply reply = grpcSymbolService.querySymbolMakerBonusConfig(GetMakerBonusConfigRequest.newBuilder()
                        .addBrokerId(broker.getOrgId()).build(), broker.getOrgId());
                List<MakerBonusConfig> makerBonusConfigLists = reply.getBonusMapMap().get(broker.getOrgId()).getConfigsList();
                for (MakerBonusConfig config : makerBonusConfigLists) {
                    tmpMakerBonusRateConfigMap.put(String.format("%s_%s", broker.getOrgId(), config.getSymbolId()), config);
                }
            }
            makerBonusConfigImmutableMap = ImmutableMap.copyOf(tmpMakerBonusRateConfigMap);
        } catch (Exception e) {
            // ignore
        }
    }

    public MakerBonusConfig getMakerBonusConfig(Long orgId, String symbolId) {
        return makerBonusConfigImmutableMap.get(String.format("%s_%s", orgId, symbolId));
    }

    @Scheduled(cron = "0 0/10 * * * ?")
    public void refreshOrderSource() throws Exception {
        List<Broker> brokerList = brokerMapper.queryAvailableBroker();
        Map<String, OrderSource> tmpOrderSourceMap = Maps.newHashMap();
        for (Broker broker : brokerList) {
            List<OrderSource> brokerOrderSourceList = orderSourceMapper.select(OrderSource.builder().orgId(broker.getOrgId()).build());
            for (OrderSource orderSource : brokerOrderSourceList) {
                if (Strings.isNullOrEmpty(orderSource.getBinarySource()) || orderSource.getBinarySource().length() < OrderSource.BINARY_ORDER_SOURCE_BITS) {
                    orderSource.setBinarySource(orderSource.transferBinarySource());
                }
                tmpOrderSourceMap.put(String.format("%s_%s", orderSource.getOrgId(), orderSource.getSource()), orderSource);
                tmpOrderSourceMap.put(String.format("%s_%s", orderSource.getOrgId(), orderSource.getSourceName()), orderSource);
            }
        }
        orderSourceMap = ImmutableMap.copyOf(tmpOrderSourceMap);
    }

    private String getPlatformBinaryValue(Platform platform) {
        String platformBinaryValue = Integer.toBinaryString(platform.getNumber());
        if (platformBinaryValue.length() < 6) {
            platformBinaryValue = PLATFORM_LEFT_PADDING0[6 - platformBinaryValue.length()] + platformBinaryValue;
        }
        return platformBinaryValue;
    }

    private String getBinaryOrderSource(Long orgId, String source) {
        if (Strings.isNullOrEmpty(source)) {
            return OrderSource.DEFAULT_BINARY_ORDER_SOURCE;
        }
        OrderSource orderSource = orderSourceMap.get(String.format("%s_%s", orgId, source));
        if (orderSource == null) {
            return OrderSource.DEFAULT_BINARY_ORDER_SOURCE;
        }
        return orderSource.getBinarySource();
    }

    public Integer getOrderSource(Long orgId, Platform platform, String source) {
        String binaryValue = getPlatformBinaryValue(platform) + getBinaryOrderSource(orgId, source);
//        log.info("orgId:{} platform:{} platformValue:{} source:{} platformBinary:{} orderSourceBinary:{} orderSourceBinary:{}, orderSource:{}",
//                orgId, platform, platform.getNumber(), source, getPlatformBinaryValue(platform), getBinaryOrderSource(orgId, source), binaryValue, Integer.valueOf(binaryValue, 2));
        return Integer.valueOf(binaryValue, 2);
    }

    private Map<String, TokenDetail> queryTokenDetailOptionMap(List<Token> tokenList, Long orgId) {
//        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenList(GetTokenListRequest.newBuilder().setForceRequest(true).build()).getTokenDetailsList();
        GetTokenIdsRequest request = GetTokenIdsRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .addAllTokenIds(tokenList.stream().map(Token::getTokenId).collect(Collectors.toList()))
                .build();
        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenListByIds(request).getTokenDetailsList();
        return tokenDetailList.stream()
                .collect(Collectors.toMap(TokenDetail::getTokenId, tokenDetail -> tokenDetail, (p, q) -> p));
    }

    @Scheduled(initialDelay = 1000, fixedRate = 2500)
    public void scheduleTokenRate() {
        try {
            orgTokenMap.keySet().parallelStream().forEach(orgId -> {
                List<Token> tokenList = orgTokenMap.get(orgId);
                Map<Long, List<Token>> exchangeTokenMap = tokenList.stream().collect(Collectors.groupingBy(Token::getExchangeId));
                for (Long exchangeId : exchangeTokenMap.keySet()) {
                    if (!CollectionUtils.isEmpty(exchangeTokenMap.get(exchangeId))) {
                        GetExchangeRateRequest request = GetExchangeRateRequest.newBuilder()
                                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                                .setExchangeId(exchangeId)
                                .addAllTokens(exchangeTokenMap.get(exchangeId).stream().map(Token::getTokenId).collect(Collectors.toList()))
                                .build();
                        try {
                            GetExchangeRateReply reply = grpcQuoteService.getRatesV3(request);
                            Map<String, Rate> tokenRateMap = reply.getExchangeRate().getRateMapMap();
                            for (String token : tokenRateMap.keySet()) {
                                TOKEN_RATE_CACHE.put(CommonUtil.getFXRateKey(orgId, token), tokenRateMap.get(token));
                            }
                        } catch (Exception e) {
                            // ignore
                            log.warn("getRateV3:{} exception", JsonUtil.defaultGson().toJson(request), e);
                        }
                    }
                }
            });
        } catch (Exception e) {
            log.error("schedule token FXRate occurred a exception", e);
        }
    }

    private Map<String, Symbol> queryTokenSymbolOptionMap(List<Symbol> symbolList) {
        /**
         * 期权约束条件：期权token对期权symbol 是1：1关系
         */
        return symbolList.stream()
                .collect(Collectors.toMap(t -> String.format("%s_%s", t.getOrgId(), t.getBaseTokenId()), symbol -> symbol, (p, q) -> p));
    }

    public io.bhex.broker.grpc.basic.Symbol getOrgSymbol(Long orgId, String symbolId) {
        String key = orgId + "_" + symbolId;
        return orgSymbolMap.get(key);
    }

    public SymbolDetail getSymbolDetailOption(Long exchangeId, String symbolId) {
        String key = exchangeId + "_" + symbolId;
        return exchangeSymbolDetailOptionMap.get(key);
    }

    public SymbolDetail getSymbolDetailFutures(Long exchangeId, String symbolId) {
        SymbolDetail symbolDetail = exchangeSymbolDetailFuturesMap.get(String.format("%s_%s", exchangeId, symbolId));
        return symbolDetail;
    }

    public SymbolDetail getSymbolDetailFuturesByOrgIdAndSymbolId(Long orgId, String symbolId) {
        Long exchangeId = getOrgExchangeFuturesMap().get(String.format("%s_%s", orgId, symbolId));
        if (exchangeId == null) {
            log.warn("get exchangeId failed. orgId: {} symbolId: {}", orgId, symbolId);
            return null;
        }
        return getSymbolDetailFutures(exchangeId, symbolId);
    }

    public ImmutableMap<String, TokenFuturesInfo> getTokenFuturesInfoMap() {
        return tokenFuturesInfoMap;
    }

    public ImmutableMap<String, Long> getOrgExchangeFuturesMap() {
        return orgExchangeFuturesMap;
    }

    public QueryKycCardTypeResponse queryKycCardType(Header header) {
        List<KycCardType> kycCardTypeList = getCardTypeTable().cellSet().stream()
                .map(cell -> KycCardType.newBuilder().setKey(cell.getRowKey())
                        .setValue(cell.getValue()).setLanguage(cell.getColumnKey()).build()).collect(Collectors.toList());
        return QueryKycCardTypeResponse.newBuilder().addAllKycCardType(kycCardTypeList).build();
    }

    private io.bhex.broker.grpc.basic.Country getCountry(Country country, CountryDetail detail) {
        return io.bhex.broker.grpc.basic.Country.newBuilder()
                .setId(country.getId())
                .setNationalCode(country.getNationalCode())
                .setShortName(country.getDomainShortName())
                .setName(detail.getCountryName())
                .setIndexName(detail.getIndexName())
                .setLanguage(detail.getLanguage())
                .setCustomOrder(country.getCustomOrder())
                .setForAreaCode(country.getForAreaCode() == 1)
                .setForNationality(country.getForNationality() == 1)
                .build();
    }

    public QueryCountryResponse queryCountries(Header header) {
        List<Country> localCountryList = countryMapper.queryAllCountry();
        List<Long> countryIdList = localCountryList.stream().map(Country::getId).collect(Collectors.toList());

        Map<Long, Country> tmpCountryMap = localCountryList.stream().collect(Collectors.toMap(Country::getId, Function.identity()));
        List<CountryDetail> countryDetailList = countryMapper.queryAllCountryDetail();
        countryDetailList = countryDetailList.stream()
                .filter(detail -> countryIdList.contains(detail.getCountryId()))
                .collect(Collectors.toList());
        List<io.bhex.broker.grpc.basic.Country> countryList = countryDetailList.stream()
                .map(item -> getCountry(tmpCountryMap.get(item.getCountryId()), item)).collect(Collectors.toList());
        return QueryCountryResponse.newBuilder().addAllCountry(countryList).build();
    }

    public String getCountryName(Long countryId, String language) {
        return countryNameTable.get(countryId, language);
    }

    public QueryTokenResponse queryTokens(List<Integer> categories, Long orgId) {
        List<Token> localTokenList = tokenMapper.queryAll(categories);

        GetTokenListRequest request = GetTokenListRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setForceRequest(true).build();
        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenList(request).getTokenDetailsList();
        Map<String, TokenDetail> tokenDetailMap = tokenDetailList.stream().collect(Collectors.toMap(TokenDetail::getTokenId, Function.identity()));
        List<io.bhex.broker.grpc.basic.Symbol> symbolList = querySymbols(orgId, categories).getSymbolList();
        List<io.bhex.broker.grpc.basic.Token> tokenList = localTokenList.stream()
                .map(token ->
                        getToken(token, tokenDetailMap.get(token.getTokenId()),
                                symbolList.stream().filter(symbol -> symbol.getBaseTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList()),
                                Lists.newArrayList()
//                                symbolList.stream().filter(symbol -> symbol.getQuoteTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList())
                        ))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return QueryTokenResponse.newBuilder().addAllTokens(tokenList).build();
    }

    public QueryCoinTokenDetailsResponse queryCoinTokenDetails(Header header) {
        List<Token> localTokenList = Lists.newArrayList();
        if (header.getOrgId() != 0) {
            localTokenList = tokenMapper.queryAllByOrgId(Lists.newArrayList(TokenCategory.MAIN_CATEGORY.getNumber(), TokenCategory.INNOVATION_CATEGORY.getNumber()), header.getOrgId());
        } else {
            localTokenList = tokenMapper.queryAll(Lists.newArrayList(TokenCategory.MAIN_CATEGORY.getNumber(), TokenCategory.INNOVATION_CATEGORY.getNumber()));
        }
        List<String> tokenIds = localTokenList.stream().map(Token::getTokenId).collect(Collectors.toList());
        List<TokenDetail> tokenDetailList;
        GetTokenIdsRequest request;
        request = GetTokenIdsRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .addAllTokenIds(tokenIds)
                .build();
        tokenDetailList = grpcTokenService.queryTokenListByIds(request).getTokenDetailsList();
//        List<TokenTypeEnum> typeList = Arrays.stream(TokenTypeEnum.values())
//                .filter(t -> t != TokenTypeEnum.UNRECOGNIZED)
//                .filter(t -> t != TokenTypeEnum.FUTURE)
//                .filter(t -> t != TokenTypeEnum.OPTION)
//                .collect(Collectors.toList());
//        GetTokenListRequest request = GetTokenListRequest.newBuilder().addAllTypes(typeList).setForceRequest(true).build();
//        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenList(request).getTokenDetailsList();

        List<io.bhex.broker.grpc.basic.CoinTokenDetail> tokenList = tokenDetailList.stream()
                .map(token -> {
                    CoinTokenDetail.Builder builder = CoinTokenDetail.newBuilder()
                            .setTokenId(token.getTokenId())
                            .setIconUrl(token.getIcon())
                            .setDescription(token.getTokenDetail())
                            .setMaxQuantitySupplied(token.getMaxQuantitySupplied())
                            .setCurrentTurnover(token.getCurrentTurnover())
                            .setOfficialWebsiteUrl(Strings.nullToEmpty(token.getOfficialWebsiteUrl()))
                            .setWhitePaperUrl(Strings.nullToEmpty(token.getWhitePaperUrl()))
                            .setPublishTime(Strings.nullToEmpty(token.getPublishTime()))
                            .setExploreUrl(token.getExploreUrl());
                    return builder.build();

                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return QueryCoinTokenDetailsResponse.newBuilder().addAllDetail(tokenList).build();
    }

    public QueryTokenResponse queryTokensByOrgId(List<Integer> categories, Long orgId) {
        List<Token> localTokenList = tokenMapper.queryAllByOrgId(categories, orgId);
        GetTokenListRequest request = GetTokenListRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setForceRequest(true).build();
        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenList(request).getTokenDetailsList();
        Map<String, TokenDetail> tokenDetailMap = tokenDetailList.stream().collect(Collectors.toMap(TokenDetail::getTokenId, Function.identity()));
        List<io.bhex.broker.grpc.basic.Symbol> symbolList = querySymbols(orgId, categories).getSymbolList();
        List<io.bhex.broker.grpc.basic.Token> tokenList = localTokenList.stream()
                .map(token ->
                        getToken(token,
                                tokenDetailMap.get(token.getTokenId()),
                                symbolList.stream().filter(symbol -> symbol.getBaseTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList()),
                                Lists.newArrayList()
//                        symbolList.stream().filter(symbol -> symbol.getQuoteTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList())
                        ))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return QueryTokenResponse.newBuilder().addAllTokens(tokenList).build();
    }

    public QueryQuoteTokenResponse queryQuoteTokens(List<Integer> categories, Long orgId) {
        List<QuoteToken> quoteTokenList = tokenMapper.queryQuoteTokens(categories);

        GetTokenListRequest request = GetTokenListRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setForceRequest(true).build();
        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenList(request).getTokenDetailsList();
        Map<String, TokenDetail> tokenDetailMap = tokenDetailList.stream().collect(Collectors.toMap(TokenDetail::getTokenId, Function.identity()));

        List<io.bhex.broker.grpc.basic.Symbol> symbolList = querySymbols(orgId, categories).getSymbolList();
        return QueryQuoteTokenResponse.newBuilder()
                .addAllQuoteTokens(
                        quoteTokenList.stream()
                                .map(token -> getQuoteToken(token, tokenDetailMap.get(token.getTokenId()),
                                        symbolList.stream().filter(symbol -> symbol.getBaseTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList()),
                                        symbolList.stream().filter(symbol -> symbol.getQuoteTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList())
                                )).collect(Collectors.toList()))
                .build();
    }

    public QueryQuoteTokenResponse queryQuoteTokensByOrgId(List<Integer> categories, Long orgId) {
        List<QuoteToken> quoteTokenList = tokenMapper.queryQuoteTokensByOrgId(categories, orgId);

        GetTokenListRequest request = GetTokenListRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setForceRequest(true).build();
        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenList(request).getTokenDetailsList();
        Map<String, TokenDetail> tokenDetailMap = tokenDetailList.stream().collect(Collectors.toMap(TokenDetail::getTokenId, Function.identity()));

        List<io.bhex.broker.grpc.basic.Symbol> symbolList = querySymbols(orgId, categories).getSymbolList();
        return QueryQuoteTokenResponse.newBuilder()
                .addAllQuoteTokens(
                        quoteTokenList.stream()
                                .map(token -> getQuoteToken(token, tokenDetailMap.get(token.getTokenId()),
                                        symbolList.stream().filter(symbol -> symbol.getBaseTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList()),
                                        symbolList.stream().filter(symbol -> symbol.getQuoteTokenId().equalsIgnoreCase(token.getTokenId())).collect(Collectors.toList())
                                )).collect(Collectors.toList()))
                .build();
    }

    public io.bhex.broker.grpc.basic.Token getToken(Long orgId, String tokenId) {
        Token token = tokenMapper.getToken(orgId, tokenId);
        TokenDetail tokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setTokenId(tokenId).build());
        return getToken(token, tokenDetail, Lists.newArrayList(), Lists.newArrayList());
    }

    public String getTokenName(Long orgId, String tokenId) {
        String key = String.format("%s_%s", orgId, tokenId);
        String tokenName = tokenNameMap.get(key);
        return Strings.isNullOrEmpty(tokenName) ? tokenId : tokenName;
    }

    private io.bhex.broker.grpc.basic.Token getToken(Token token, TokenDetail tokenDetail,
                                                     List<io.bhex.broker.grpc.basic.Symbol> baseTokenSymbols,
                                                     List<io.bhex.broker.grpc.basic.Symbol> quoteTokenSymbols) {
        if (tokenDetail == null) {
            return null;
        }
        if (!Strings.isNullOrEmpty(tokenDetail.getParentTokenId())) { //子链币不展示在前台
            return null;
        }
        io.bhex.broker.grpc.basic.Token.Builder tokenBuilder = io.bhex.broker.grpc.basic.Token.newBuilder()
                .setOrgId(token.getOrgId())
                .setTokenId(token.getTokenId())
                .setTokenName(token.getTokenName())
                .setTokenFullName(token.getTokenFullName())
                .setIconUrl(Strings.isNullOrEmpty(tokenDetail.getIcon()) ? token.getTokenIcon() : tokenDetail.getIcon())
                .setAllowDeposit(tokenDetail.getAllowDeposit()
                        && token.getAllowDeposit() == BrokerServerConstants.ALLOW_STATUS.intValue()
                        && token.getStatus() == BrokerServerConstants.ONLINE_STATUS.intValue()
                        && (!tokenDetail.getIsAggregate() || aggregateTokenAllowDepositList(token.getOrgId()).contains(token.getTokenId())))
                .setAllowWithdraw(tokenDetail.getAllowWithdraw()
                        && token.getAllowWithdraw() == BrokerServerConstants.ALLOW_STATUS.intValue()
                        && token.getStatus() == BrokerServerConstants.ONLINE_STATUS.intValue()
                        && (!tokenDetail.getIsAggregate() || aggregateTokenAllowDepositList(token.getOrgId()).contains(token.getTokenId())))
                .setMinDepositQuantity(DecimalUtil.toBigDecimal(tokenDetail.getDepositMinQuantity())
                        .stripTrailingZeros().toPlainString())
                .addAllNewBaseTokenSymbols(baseTokenSymbols)
                .addAllNewQuoteTokenSymbols(quoteTokenSymbols)
                .setIsEos(tokenDetail.getType() == TokenTypeEnum.EOS_TOKEN)
                .setTokenType(tokenDetail.getType().name())
                .setNeedKycQuantity(token.getNeedKycQuantity().stripTrailingZeros().toPlainString())
                .setNeedAddressTag(tokenDetail.getAddressNeedTag())
                .setExchangeId(token.getExchangeId())
                .setCustomOrder(token.getCustomOrder())
                .setIsHighRiskToken(token.getIsHighRiskToken() == 1)
                .addAllTokenChainInfo(tokenDetail.getChainTypesList().stream()
                        .map(chainType -> TokenChainInfo.newBuilder()
                                .setChainType(chainType.getChainName())
                                .setAllowDeposit(chainType.getAllowDeposit())
                                .setAllowWithdraw(chainType.getAllowWithdraw())
                                .build()).collect(Collectors.toList()))
                .putAllExtraTag(ExtraTagUtil.newInstance(token.getExtraTag()).map())
                .putAllExtraConfig(ExtraConfigUtil.newInstance(token.getExtraConfig()).map())
                .setIsTest(tokenDetail.getIsTest())
                .setIsAggragate(tokenDetail.getIsAggregate());
//                .setDescription(tokenDetail.getTokenDetail())
//                .setMaxQuantitySupplied(DecimalUtil.toBigDecimal(tokenDetail.getMaxQuantitySupplied()).stripTrailingZeros().toPlainString())
//                .setCurrentTurnover(DecimalUtil.toBigDecimal(tokenDetail.getCurrentTurnover()).stripTrailingZeros().toPlainString())
//                .setOfficialWebsiteUrl(Strings.nullToEmpty(tokenDetail.getOfficialWebsiteUrl()))
//                .setWhitePaperUrl(Strings.nullToEmpty(tokenDetail.getWhitePaperUrl()))
//                .setPublishTime(Strings.nullToEmpty(tokenDetail.getPublishTime()))
//                .setExploreUrl(tokenDetail.getExploreUrl());
        Optional.ofNullable(tokenDetail.getTokenOptionInfo()).ifPresent(tokenOption -> {
            tokenBuilder.setTokenOptionInfo(OptionUtil.toTokenOptionInfo(tokenOption));
        });
        return tokenBuilder.build();
    }

    public List<String> aggregateTokenAllowDepositList(long brokerId) {
        BaseBizConfigService.ConfigData config = baseBizConfigService.getBrokerBaseConfig(brokerId,
                "saas.broker.switch", "aggregate_allow_deposit_withdraw", null);
        List<String> allowDepositWithdrawList = config != null ? Lists.newArrayList(config.getValue().split(",")) : Lists.newArrayList();
        return allowDepositWithdrawList;
    }

    private void setTokenOptionInfo(TokenDetail tokenDetail, io.bhex.broker.grpc.basic.Token.Builder tokenBuilder) {
        TokenOptionInfo tokenOptionInfo = tokenDetail.getTokenOptionInfo();
        if (tokenOptionInfo != null && StringUtils.isNotEmpty(tokenOptionInfo.getTokenId())) {
            tokenBuilder.setTokenOptionInfo(getTokenOptionInfo(tokenOptionInfo));
        }
    }

    private io.bhex.broker.grpc.basic.TokenOptionInfo getTokenOptionInfo(TokenOptionInfo tokenOption) {
        return io.bhex.broker.grpc.basic.TokenOptionInfo.newBuilder()
                .setTokenId(tokenOption.getTokenId())
                .setStrikePrice(DecimalUtil.toTrimString(tokenOption.getStrikePrice()))
                .setIssueDate(tokenOption.getIssueDate())
                .setSettlementDate(tokenOption.getSettlementDate())
                .setIsCall(tokenOption.getIsCall())
                .setMaxPayOff(DecimalUtil.toTrimString(tokenOption.getMaxPayOff()))
                .setCoinToken(tokenOption.getCoinToken())
                .setIndexToken(tokenOption.getIndexToken())
                .setUnderlyingId(tokenOption.getUnderlyingId())
                .build();
    }

    private io.bhex.broker.grpc.basic.QuoteToken getQuoteToken(QuoteToken token, TokenDetail tokenDetail,
                                                               List<io.bhex.broker.grpc.basic.Symbol> baseTokenSymbols,
                                                               List<io.bhex.broker.grpc.basic.Symbol> quoteTokenSymbols) {
        return io.bhex.broker.grpc.basic.QuoteToken.newBuilder()
                .setOrgId(token.getOrgId())
                .setTokenId(token.getTokenId())
                .setTokenName(token.getTokenName())
                .setIconUrl(tokenDetail == null ? token.getTokenIcon() : (Strings.isNullOrEmpty(tokenDetail.getIcon()) ? token.getTokenIcon() : tokenDetail.getIcon()))
                .setCustomOrder(token.getCustomOrder())
                .addAllNewBaseTokenSymbols(baseTokenSymbols)
                .addAllNewQuoteTokenSymbols(quoteTokenSymbols)
                .build();
    }

    public QuerySymbolResponse querySymbols(Long orgId, List<Integer> categories) {
        List<io.bhex.broker.grpc.basic.Symbol> symbolList = querySymbolList(orgId, categories);
        return QuerySymbolResponse.newBuilder().addAllSymbol(symbolList).build();
    }

    public QuerySymbolResponse querySymbolsWithTokenDetail(Long orgId, List<Integer> categories) {
        List<io.bhex.broker.grpc.basic.Symbol> symbolList = querySymbolList(orgId, categories);
        symbolList = symbolList
                .stream()
                .map(this::getSymbolWithTokenDetail)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return QuerySymbolResponse.newBuilder().addAllSymbol(symbolList).build();
    }

    private io.bhex.broker.grpc.basic.Symbol getSymbolWithTokenDetail(io.bhex.broker.grpc.basic.Symbol symbol) {
        if (symbol.getCategory() == TokenCategory.OPTION_CATEGORY.getNumber()) {
            TokenOptionInfo tokenOption = getTokenOptionInfo(symbol.getBaseTokenId());
            if (tokenOption != null) {
                if (new DateTime(tokenOption.getSettlementDate()).plusMinutes(5).getMillis() < new Date().getTime()) {
                    return null;
                }
                return symbol.toBuilder().mergeTokenOption(getTokenOptionInfo(tokenOption)).build();
            }
        }
        return symbol;
    }

    public List<SymbolDetail> querySymbolDetailList(Long orgId, List<Integer> categories) { //todo: 待重构
        List<BrokerExchange> brokerExchangeList = listAllContract(orgId);
        List<SymbolDetail> symbolDetailList = Lists.newArrayList();
        List<Long> exchangeIds = brokerExchangeList.stream().map(BrokerExchange::getExchangeId)
                .distinct().collect(Collectors.toList());
        GetSymbolMapRequest getSymbolMapRequest = GetSymbolMapRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .addAllExchangeIds(exchangeIds).setForceRequest(true).build();
        GetSymbolMapReply getSymbolMapReply = grpcSymbolService.querySymbolList(getSymbolMapRequest);
        for (Long exchangeId : exchangeIds) {
            SymbolList symbolList = getSymbolMapReply.getSymbolMapMap().get(exchangeId);
            if (CollectionUtils.isEmpty(symbolList.getSymbolDetailsList())) {
                continue;
            }
            symbolDetailList.addAll(symbolList.getSymbolDetailsList());
        }
        return symbolDetailList;
    }

    public List<io.bhex.broker.grpc.basic.Symbol> querySymbolList(Long orgId, List<Integer> categories) { //todo: 待重构
        List<BrokerExchange> brokerExchangeList = listAllContract(orgId);
        List<SymbolDetail> symbolDetailList = Lists.newArrayList();
        List<Long> exchangeIds = brokerExchangeList.stream().map(BrokerExchange::getExchangeId)
                .distinct().collect(Collectors.toList());
        GetSymbolMapRequest getSymbolMapRequest = GetSymbolMapRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .addAllExchangeIds(exchangeIds).setForceRequest(true).build();
        GetSymbolMapReply getSymbolMapReply = grpcSymbolService.querySymbolList(getSymbolMapRequest);
        for (Long exchangeId : exchangeIds) {
            SymbolList symbolList = getSymbolMapReply.getSymbolMapMap().get(exchangeId);
            if (CollectionUtils.isEmpty(symbolList.getSymbolDetailsList())) {
                continue;
            }
            symbolDetailList.addAll(symbolList.getSymbolDetailsList());
        }

        Map<String, SymbolDetail> symbolDetailMap = new HashMap<>();

        for (SymbolDetail sd : symbolDetailList) {
            if (sd == null || sd.getExchangeId() == 0 || sd.getSymbolId() == null) {
                log.warn("symbol detail is empty");
                continue;
            }
            long exchangeId = sd.getExchangeId();
            String symbolId = sd.getSymbolId();
            symbolDetailMap.put(exchangeId + "_" + symbolId, sd);
        }
        List<Symbol> localSymbolList = symbolMapper.queryOrgSymbols(orgId, categories);
        List<io.bhex.broker.grpc.basic.Symbol> symbolList = localSymbolList.stream()
                .map(symbol -> getSymbol(symbol, symbolDetailMap.get(symbol.getExchangeId() + "_" + symbol.getSymbolId())))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
//        if (initCoinData) {
//            Map<String, io.bhex.broker.grpc.basic.Symbol> tmpOrgSymbolMap = Maps.newHashMap();
//            if (!CollectionUtils.isEmpty(symbolList)) {
//                symbolList.forEach(symbol -> {
//                    String key = symbol.getOrgId() + "_" + symbol.getSymbolId();
//                    tmpOrgSymbolMap.put(key, symbol);
//                });
//            }
//            BasicService.orgSymbolMap = ImmutableMap.copyOf(tmpOrgSymbolMap);
//        }
//        if (initOptionData) {
//            Map<String, SymbolDetail> tmpSymbolMap = Maps.newHashMap();
//            if (!CollectionUtils.isEmpty(symbolList)) {
//                symbolList.forEach(symbol -> {
//                    String key = symbol.getExchangeId() + "_" + symbol.getSymbolId();
//                    tmpSymbolMap.put(key, symbolDetailMap.get(symbol.getExchangeId() + "_" + symbol.getSymbolId()));
//                });
//            }
//            BasicService.exchangeSymbolDetailOptionMap = ImmutableMap.copyOf(tmpSymbolMap);
//        }
        return symbolList;
    }

    /**
     * 获取全部的合作关系 由于saas调用结构调整，所有机构合作关系都从saas获取。
     */
    public List<BrokerExchange> listAllContract() {
        List<Broker> brokerList = brokerMapper.selectAll();
        List<Long> brokerIds = brokerList.stream()
                .filter(broker -> broker.getStatus() == 1)
                .map(Broker::getOrgId).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(brokerIds)) {
            ListAllContractByOrgIdsRequest request = ListAllContractByOrgIdsRequest.newBuilder()
                    .setApplyOrgType(OrgType.Broker_Org)
                    .addAllOrgIds(brokerIds)
                    .build();
            return getListAllContractByOrgIds(request);
        }
        return new ArrayList<>();
    }

    private List<BrokerExchange> getListAllContractByOrgIds(ListAllContractByOrgIdsRequest request) {
        ListContractReply reply = orgContractService.listAllContract(request);
        List<ContractDetail> contractList = reply.getContractDetailList();
        List<BrokerExchange> list = new ArrayList<>();
        if (!CollectionUtils.isEmpty(contractList)) {
            list.addAll(contractList.stream().map(contract -> {
                BrokerExchange brokerExchange = new BrokerExchange();
                brokerExchange.setOrgId(contract.getBrokerId());
                brokerExchange.setExchangeId(contract.getExchangeId());
                brokerExchange.setExchangeName(contract.getContractOrgName());
                return brokerExchange;
            }).collect(Collectors.toList()));
        }
        return list;
    }

    public List<BrokerExchange> listAllContract(Long orgId) {
        if (orgId != null && orgId > 0) {
            ListAllContractByOrgIdsRequest request = ListAllContractByOrgIdsRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setApplyOrgType(OrgType.Broker_Org)
                    .addAllOrgIds(Collections.singletonList(orgId))
                    .build();
            ListContractReply reply = orgContractService.listAllContract(request);
            List<ContractDetail> contractList = reply.getContractDetailList();
            if (!CollectionUtils.isEmpty(contractList)) {
                return contractList.stream().map(contract -> {
                    BrokerExchange brokerExchange = new BrokerExchange();
                    brokerExchange.setOrgId(contract.getBrokerId());
                    brokerExchange.setExchangeId(contract.getExchangeId());
                    brokerExchange.setExchangeName(contract.getContractOrgName());
                    return brokerExchange;
                }).collect(Collectors.toList());
            }
            return new ArrayList<>();
        }
        return listAllContract();
    }

    public String getSymbolName(Long orgId, String symbolId) {
        return symbolNameMap.getOrDefault(String.format("%s_%s", orgId, symbolId), symbolId);
    }

    private io.bhex.broker.grpc.basic.Symbol getSymbol(Symbol symbol, SymbolDetail symbolDetail) {
        if (symbolDetail == null) {
            return null;
        }
//        if (symbol.getNeedPreviewCheck() == BrokerServerConstants.NEED_PREVIEW_CHECK) {
//            if (!brokerServerProperties.getIsPreview() && System.currentTimeMillis() < symbol.getOpenTime()) {
//                return null;
//            }
//        }
        return io.bhex.broker.grpc.basic.Symbol.newBuilder()
                .setOrgId(symbol.getOrgId())
                .setExchangeId(symbol.getExchangeId())
                .setMatchExchangeId(symbolDetail.getMatchExchangeId())
                .setSymbolId(Strings.nullToEmpty(symbol.getSymbolId()).trim())
                .setSymbolName(symbol.getSymbolName())
                .setBaseTokenId(symbol.getBaseTokenId())
                .setBaseTokenName(symbol.getBaseTokenName())
                .setQuoteTokenId(symbol.getQuoteTokenId())
                .setQuoteTokenName(symbol.getQuoteTokenName())
                .setBasePrecision(DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().toPlainString())
                .setQuotePrecision(DecimalUtil.toBigDecimal(symbolDetail.getQuotePrecision()).stripTrailingZeros().toPlainString())
                .setMinTradeQuantity(DecimalUtil.toBigDecimal(symbolDetail.getMinTradeQuantity()).stripTrailingZeros().toPlainString())
                .setMinTradeAmount(DecimalUtil.toBigDecimal(symbolDetail.getMinTradeAmount()).stripTrailingZeros().toPlainString())
                .setMinPricePrecision(DecimalUtil.toBigDecimal(symbolDetail.getMinPricePrecision()).stripTrailingZeros().toPlainString())
                .setDigitMerge(symbolDetail.getDigitMergeList())
                .setCanTrade(symbolDetail.getSaasAllowTradeStatus() //saas设置
                        && symbolDetail.getAllowTrade() //交易所设置
                        && symbol.getAllowTrade() == BrokerServerConstants.ALLOW_STATUS.intValue() //broker设置
                        && symbol.getStatus() == BrokerServerConstants.ONLINE_STATUS.intValue()) //broker设置
                .setBrokerAllowTrade(symbol.getAllowTrade() == BrokerServerConstants.ALLOW_STATUS.intValue())
                .setBrokerOnlineStatus(symbol.getStatus() == BrokerServerConstants.ONLINE_STATUS.intValue())
                .setExchangeAllowTrade(symbolDetail.getAllowTrade())
                .setSaasAllowTrade(symbolDetail.getSaasAllowTradeStatus())
                .setBanSellStatus(symbol.getBanSellStatus() == 1)
                .setBanBuyStatus(symbol.getBanBuyStatus() == 1)
                .setShowStatus(symbol.getCategory() == 4 ? symbol.getStatus() == 1 : symbol.getShowStatus() == 1) //合约发布则展示
                .setCustomOrder(symbol.getCustomOrder())
                .setIndexShow(symbol.getIndexShow() == 1)
                .setIndexShowOrder(symbol.getIndexShowOrder())
                .setCategory(symbol.getCategory())
                .setCheckInPreview(symbol.getNeedPreviewCheck() == 1)
                .setOpenTime(symbol.getOpenTime())
                .setIndexToken(symbolDetail.getIndexToken())
                .setIsReverse(symbolDetail.getIsReverse())
                .setIndexRecommendOrder(symbol.getIndexRecommendOrder())
                .setIsAggragate(symbolDetail.getIsAggregate())
                .setIsTest(symbolDetail.getIsTest())
                .setAllowMargin(symbol.getAllowMargin() == 1)
                .setFilterTime(symbol.getFilterTime())
                .setFilterTopStatus(symbol.getFilterTopStatus() == 1)
                .setCustomLabelId(symbol.getLabelId())
                .setHideFromOpenapi(symbol.getHideFromOpenapi() == 1)
                .setForbidOpenapiTrade(symbol.getForbidOpenapiTrade() == 1)
                .putAllExtraTag(ExtraTagUtil.newInstance(symbol.getExtraTag()).map())
                .putAllExtraConfig(ExtraConfigUtil.newInstance(symbol.getExtraConfig()).map())
                .setAllowPlan(symbol.getAllowPlan() == 1)
                .build();
    }

    public void validNationalCode(String nationalCode) {
        if (Strings.isNullOrEmpty(nationalCode)) {
            throw new BrokerException(BrokerErrorCode.NATIONAL_CODE_CANNOT_BE_NULL);
        }
        if (!nationalCodeList.contains(nationalCode)) {
            throw new BrokerException(BrokerErrorCode.NATIONAL_CODE_ERROR);
        }
    }

    public void validCountryId(Long countryId) {
        if (countryId == null) {
            throw new BrokerException(BrokerErrorCode.NATIONALITY_CANNOT_BE_NULL);
        }
        if (!countryIdList.contains(countryId)) {
            throw new BrokerException(BrokerErrorCode.NATIONALITY_ERROR);
        }
    }

    public String getCountryCode(Long countryId) {
        return countryCodeMap.getOrDefault(countryId, "");
    }

    public GetServerTimeResponse getServerTime(Long serverSleepTime) throws InterruptedException {
        if (serverSleepTime > 0) {
            log.info("getServerTimeResponse sleeps for {} milliseconds", serverSleepTime);
            Thread.sleep(serverSleepTime);
        }
        return GetServerTimeResponse.newBuilder().setServerTime(System.currentTimeMillis()).build();
    }

    public List<SensitiveWords> getAllSensitiveWords() {
        SensitiveWords example = SensitiveWords.builder()
                .status(0)
                .build();
        return sensitiveWordsMapper.select(example);
    }

    public Rate getV3Rate(Long orgId, String tokenId) {
        Rate rate = TOKEN_RATE_CACHE.getIfPresent(CommonUtil.getFXRateKey(orgId, tokenId));
        if (rate == null) {
            log.warn("orgId:{} token: {} cannot find rate config", orgId, tokenId);
            return null;
        }
        return rate;
    }

    public JsonObject getOrgTokenRate(Long orgId, String tokenId) {
        JsonObject dataObj = new JsonObject();
        Token token = tokenMapper.getToken(orgId, tokenId);
        dataObj.addProperty("exchangeId", token.getExchangeId());
        GetExchangeRateRequest request = GetExchangeRateRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setExchangeId(token.getExchangeId())
                .addTokens(tokenId)
                .build();
        try {
            GetExchangeRateReply reply = grpcQuoteService.getRatesV3(request);
            Map<String, Rate> tokenRateMap = reply.getExchangeRate().getRateMapMap();
            if (tokenRateMap.containsKey(tokenId)) {
                dataObj.addProperty("rate", JsonUtil.defaultGson().toJson(tokenRateMap.get(tokenId)));
            } else {
                dataObj.addProperty("rate", "no rate data");
            }
        } catch (Exception e) {
            // ignore
            log.warn("get org token RateV3:{} exception", JsonUtil.defaultGson().toJson(request), e);
        }
        return dataObj;
    }

//    @Deprecated
//    public FXRate getFXRate(Long orgId, String tokenId) {
//        FXRate fxRate = FX_RATE_CACHE.getIfPresent(CommonUtil.getFXRateKey(orgId, tokenId));
//        if (fxRate == null) {
//            log.warn("orgId:{} token: {} cannot find rate config", orgId, tokenId);
//        }
//        return fxRate;
//    }

    public BigDecimal getFXRate(Long orgId, String tokenId, String targetTokenId) {
        Rate rate = this.getV3Rate(orgId, tokenId);
        if (rate == null) {
            return BigDecimal.ZERO;
        }
        switch (targetTokenId) {
            case "BTC":
            case "USDT":
            case "ETH":
                return DecimalUtil.toBigDecimal(rate.getRatesMap().get(targetTokenId));
            default:
                BigDecimal tokenUSDTRate = DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
                Rate targetRate = this.getV3Rate(orgId, targetTokenId);
                if (targetRate == null) {
                    return BigDecimal.ZERO;
                }
                BigDecimal targetTokenUSDTRate = DecimalUtil.toBigDecimal(targetRate.getRatesMap().get("USDT"));
                if (tokenUSDTRate.compareTo(BigDecimal.ZERO) == 0
                        || targetTokenUSDTRate.compareTo(BigDecimal.ZERO) == 0) {
                    return BigDecimal.ZERO;
                }
                return tokenUSDTRate.divide(targetTokenUSDTRate, 8, BigDecimal.ROUND_DOWN);
        }
    }

    public ImmutableMap<String, Symbol> getTokenSymbolOptionMap() {
        return tokenSymbolOptionMap;
    }

    public Token getOrgTokenDetail(Long orgId, String tokenId) {
        return orgTokenDetailMap.get(orgId + "_" + tokenId);
    }

    public ImmutableMap<String, TokenDetail> getTokenDetailOptionMap() {
        return tokenDetailOptionMap;
    }

    /**
     * 获取期权的配置信息
     *
     * @param tokenId token id
     * @return 配置信息
     */
    public TokenOptionInfo getTokenOptionInfo(String tokenId) {
        TokenDetail tokenDetail = tokenDetailOptionMap.get(tokenId);
        if (tokenDetail == null) {
            return null;
        }
        return tokenDetail.getTokenOptionInfo();
    }

    /**
     * 获取期权（期货）标的信息
     *
     * @param type type = 1 期权, type =2 期货
     * @return 标的信息
     */
    public List<Underlying> getUnderlyings(int type, Long orgId) {
        GetUnderlyingRequest request = GetUnderlyingRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setType(UnderlyingTypeEnum.forNumber(type))
                .build();
        GetUnderlyingReply reply = grpcTokenService.getUnderlyingList(request);
        return Optional.ofNullable(reply.getUnderlyingsList())
                .orElse(new ArrayList<>())
                .stream()
                .map(FuturesUtil::toUnderlying)
                .collect(Collectors.toList());
    }

    public List<String> getOptionCoinTokenIds() {
        return tokenDetailOptionMap.values()
                .stream()
                .map(tokenDetail -> {
                    TokenOptionInfo info = tokenDetail.getTokenOptionInfo();
                    return info != null ? info.getCoinToken() : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<FuturesRiskLimitWhiteList> getFuturesRiskLimitWhiteList(Long orgId) {
        List<FuturesRiskLimitWhiteList> whiteLists = ORG_FUTURES_RISKLIMIT_WHITELIST_CACHE.getIfPresent(orgId);
        if (whiteLists != null) {
            return whiteLists;
        }

        try {
            CommonIni commonIni = commonIniService.getCommonIniFromCache(orgId, COMMON_INI_KEY_RISKLIMIT_WHITELIST);
            if (commonIni == null || StringUtils.isEmpty(commonIni.getIniValue())) {
                whiteLists = Lists.newArrayList();
            } else {
                whiteLists = JsonUtil.defaultGson().fromJson(commonIni.getIniValue(),
                        new TypeToken<List<FuturesRiskLimitWhiteList>>() {
                        }.getType());
            }
        } catch (Throwable e) {
            log.error("getFuturesRiskLimitWhiteList error", e);
        } finally {
            if (whiteLists == null) {
                whiteLists = Lists.newArrayList();
            }
            ORG_FUTURES_RISKLIMIT_WHITELIST_CACHE.put(orgId, whiteLists);
        }

        return whiteLists;
    }

    public boolean filterRiskLimit(Header header, Long riskLimitId, boolean ignoreCheckUserId) {
        try {
            List<FuturesRiskLimitWhiteList> whiteLists = getFuturesRiskLimitWhiteList(header.getOrgId());
            if (org.apache.commons.collections4.CollectionUtils.isEmpty(whiteLists)) {
                return true;
            }
            /*
             * 如果header中的userId=0,
             */
            for (FuturesRiskLimitWhiteList whiteList : whiteLists) {
                if (whiteList.getRiskLimitId().equals(riskLimitId)) {
                    if (ignoreCheckUserId || header.getUserId() == 0) {
                        return false;
                    }
                    return whiteList.getUserIdList().contains(header.getUserId());
                }
            }
            return true;
        } catch (Throwable e) {
            log.warn("filterRiskLimit catch error: {}", e.getMessage());
            return true;
        }
    }

    public Symbol getBrokerFuturesSymbol(Long orgId, String symbolId) {
        return brokerOrgFuturesSymbolMap.get(String.format("%s_%s", orgId, symbolId));
    }

    /**
     * 通过orgId获取对应broker前端展示的币对列表，提供给quote
     * <p>
     * 为了与券商的querySymbols接口数据一致，本地拿到的symbol列表要与bh tb_exchange_symbol匹配后才返回
     */
    public QuerySymbolResponse queryBrokerOpenSymbols(Long orgId) {
        Example example = Example.builder(Symbol.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        // 页面启用 status=1
        criteria.andEqualTo("status", 1);
        List<Symbol> symbols = symbolMapper.selectByExample(example);
        List<io.bhex.broker.grpc.basic.Symbol> symbolList = symbols.stream()
                .map(this::getSimpleSymbol)
                .filter(Objects::nonNull)
                .filter(s -> Objects.nonNull(getOrgSymbol(orgId, s.getSymbolId())) || s.getCategory() == Symbol.FUTURES_CATEGORY || s.getCategory() == Symbol.OPTION_CATEGORY)
                .collect(Collectors.toList());

        return QuerySymbolResponse.newBuilder()
                .addAllSymbol(symbolList)
                .build();
    }

    public List<KycLevel> getKycLevelList() {
        return kycLevelMap.values().asList();
    }

    public KycLevel getKycLevel(Integer levelId) {
        return kycLevelMap.get(levelId);
    }

    public List<KycLevelConfig> getAllKycLevelConfigList() {
        return kycLevelConfigMapper.selectAll()
                .stream()
                .filter(this::fiterKycLevelConfig)
                .map(this::toKycLevelConfig)
                .collect(Collectors.toList());
    }

    private void refreshKycLevelMap() {
        Map<Integer, KycLevel> kycLevelMapTmp = kycLevelMapper.selectAll().stream().map(k -> KycLevel.newBuilder()
                .setLevelId(k.getLevelId())
                .setLevelName(k.getLevelName())
                .setDisplayLevel(k.getDisplayLevel())
                .setMemo(Strings.nullToEmpty(k.getMemo()))
                .setPrecondition(Strings.nullToEmpty(k.getPrecondition()))
                .build()).collect(Collectors.toMap(KycLevel::getLevelId, k -> k));
        BasicService.kycLevelMap = ImmutableMap.copyOf(kycLevelMapTmp);
    }

    private boolean fiterKycLevelConfig(io.bhex.broker.server.model.KycLevelConfig k) {
        // orgId为0表示默认配置
        if (k.getOrgId().equals(0L)) {
            return true;
        }

        BrokerKycConfig brokerKycConfig = getBrokerKycConfig(k.getOrgId(), k.getCountryId());
        if (brokerKycConfig == null) {
            brokerKycConfig = BrokerKycConfig.newDefaultInstance(k.getOrgId());
        }

        if (k.getKycLevel() / 10 == 2) {
            return brokerKycConfig.getSecondKycLevel().equals(k.getKycLevel());
        } else {
            return true;
        }
    }

    private KycLevelConfig toKycLevelConfig(io.bhex.broker.server.model.KycLevelConfig k) {
        return KycLevelConfig.newBuilder()
                .setOrgId(k.getOrgId())
                .setKycLevel(k.getKycLevel())
                .setCountryCode(countryCodeMap.get(k.getCountryId()) == null ? "" : countryCodeMap.get(k.getCountryId()))
                .setAllowOtc(k.getAllowOtc() == 1)
                .setFaceCompare(k.getFaceCompare() == 1)
                .setOtcDailyLimit(k.getOtcDailyLimit() == null ? "" : k.getOtcDailyLimit().stripTrailingZeros().toPlainString())
                .setOtcLimitCurrency(Strings.nullToEmpty(k.getOtcLimitCurrency()))
                .setWithdrawDailyLimit(k.getWithdrawDailyLimit() == null ? "" : k.getWithdrawDailyLimit().stripTrailingZeros().toPlainString())
                .setWithdrawLimitToken(Strings.nullToEmpty(k.getWithdrawLimitToken()))
                .setMemo(Strings.nullToEmpty(k.getMemo()))
                .build();
    }

    public Country getCountryByCode(String countryCode) {
        return countryDomainShortNameMap.get(countryCode);
    }

    /**
     * 根据券商ID获取中国的KYC券商配置
     */
    public BrokerKycConfig getBrokerKycConfig(Long orgId) {
        return getBrokerKycConfig(orgId, 1L, null);
    }

    /**
     * 根据券商ID获取KYC券商配置，如果没有则返回默认海外的券商配置（countryId为0）
     */
    public BrokerKycConfig getBrokerKycConfig(Long orgId, Long countryId) {
        return getBrokerKycConfig(orgId, countryId, 0L);
    }

    public BrokerKycConfig getBrokerKycConfig(Long orgId, Long countryId, Long defaultCountryId) {
        BrokerKycConfig brokerKycConfig = brokerKycConfigMap.get(String.format("%s_%s", orgId, countryId));
        if (brokerKycConfig == null && defaultCountryId != null) {
            // 如果查不到对应国家的配置，则默认取海外配置
            brokerKycConfig = brokerKycConfigMap.get(String.format("%s_%s", orgId, defaultCountryId));
        }
        return brokerKycConfig;
    }

    public List<BrokerKycConfig> getAllBrokerKycConfigs() {
        return brokerKycConfigMap.values().asList();
    }

    public io.bhex.broker.grpc.basic.BrokerKycConfig toProtoBrokerKycConfig(BrokerKycConfig config) {
        return io.bhex.broker.grpc.basic.BrokerKycConfig.newBuilder()
                .setOrgId(config.getOrgId())
                .setCountryCode(getCountryCode(config.getCountryId()))
                .setSecondKycLevel(config.getSecondKycLevel())
                .build();
    }

    private io.bhex.broker.grpc.basic.Symbol getSimpleSymbol(Symbol symbol) {
        if (symbol == null) {
            return null;
        }
        return io.bhex.broker.grpc.basic.Symbol.newBuilder()
                .setOrgId(symbol.getOrgId())
                .setExchangeId(symbol.getExchangeId())
                .setSymbolId(symbol.getSymbolId())
                .setSymbolName(symbol.getSymbolName())
                .setBaseTokenId(symbol.getBaseTokenId())
                .setBaseTokenName(symbol.getBaseTokenName())
                .setQuoteTokenId(symbol.getQuoteTokenId())
                .setQuoteTokenName(symbol.getQuoteTokenName())
                .setBanSellStatus(symbol.getBanSellStatus() == 1)
                .setCustomOrder(symbol.getCustomOrder())
                .setIndexShow(symbol.getIndexShow() == 1)
                .setIndexShowOrder(symbol.getIndexShowOrder())
                .setCategory(symbol.getCategory())
                .setCheckInPreview(symbol.getNeedPreviewCheck() == 1)
                .setOpenTime(symbol.getOpenTime())
                .setIndexRecommendOrder(symbol.getIndexRecommendOrder())
                .build();
    }

    /**
     * 从数据库获取证件类型列表
     */
    private void refreshCardTypeTable() {
        Map<Integer, CardType> cardTypeMap = cardTypeMapper.selectAll().stream()
                .filter(c -> c.getStatus().equals(1))
                .collect(Collectors.toMap(CardType::getTypeId, c -> c));

        ImmutableTable.Builder<Integer, String, String> cardTypeTableBuilder = new ImmutableTable.Builder<>();
        cardTypeLocaleMapper.selectAll().stream()
                .filter(c -> cardTypeMap.containsKey(c.getCardTypeId()))
                .forEach(c -> {
                    cardTypeTableBuilder.put(c.getCardTypeId(), c.getLocale(), c.getName());
                });
        cardTypeTable = ImmutableTable.copyOf(cardTypeTableBuilder.build());
    }

    public ImmutableTable<Integer, String, String> getCardTypeTable() {
        return cardTypeTable;
    }

    public void setSymbolTradeStatus(Header header, String symbolId, Integer status) {
        Symbol symbol = symbolMapper.getOrgSymbol(header.getOrgId(), symbolId);
        if (symbol.getCategory() > 2) {
            return;
        }
        if (status > 0) {
            status = 1;
        } else {
            status = 0;
        }
        Symbol.SymbolBuilder updateObjBuilder = Symbol.builder()
                .id(symbol.getId())
                .allowTrade(status)
                .updated(System.currentTimeMillis());
        if (status == 1) {
            updateObjBuilder.banBuyStatus(0).banSellStatus(0);
        } else {
            updateObjBuilder.banBuyStatus(1).banSellStatus(1);
        }

        Symbol updateObj = updateObjBuilder.build();
        log.info("change symbol tradeStatus, {}", JsonUtil.defaultGson().toJson(updateObj));
        symbolMapper.updateByPrimaryKeySelective(updateObj);
    }


    public List<Country> listAllCountry() {
        return countryMapper.queryAllCountry();
    }

    public TokenConfig getOrgMarginToken(Long orgId, String tokenId) {
        return orgMarginTokenMap.get(orgId + "_" + tokenId);
    }

    public List<RiskConfig> getOrgMarginRiskConfig(Long orgId) {
        return orgMarginRiskConfig.getOrDefault(orgId, new ArrayList<>());
    }

    public List<io.bhex.broker.grpc.basic.GetEtfSymbolPriceResponse.EtfPrice> getEtfSymbolPrice(io.bhex.broker.grpc.basic.GetEtfSymbolPriceRequest request) {

        io.bhex.base.token.GetEtfSymbolPriceRequest r = io.bhex.base.token.GetEtfSymbolPriceRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(request.getHeader().getOrgId()).build())
                .setExchangeId(request.getExchangeId())
                .addAllSymbolIds(request.getSymbolIdsList())
                .build();
        List<GetEtfSymbolPriceReply.EtfPrice> list = grpcSymbolService.getEtfSymbolPrice(r);
        List<GetEtfSymbolPriceResponse.EtfPrice> result = list.stream().map(e -> {
            GetEtfSymbolPriceResponse.EtfPrice.Builder builder = GetEtfSymbolPriceResponse.EtfPrice.newBuilder();
            builder.setId(e.getId());
            builder.setExchangeId(e.getExchangeId());
            builder.setEtfPrice(e.getEtfPrice());
            builder.setUnderlyingPrice(e.getUnderlyingPrice());
            builder.setTime(e.getTime());
            builder.setSymbolId(e.getSymbolId());
            builder.setUnderlyingIndexId(e.getUnderlyingIndexId());
            builder.setContractSymbolId(e.getContractSymbolId());
            builder.setIsLong(e.getIsLong());
            builder.setLeverage(e.getLeverage());
            return builder.build();
        }).collect(Collectors.toList());
        return result;
    }

    public BigDecimal getIndices(String symbol, Long orgId) {
        String key = String.format("indices:%s:%s", orgId, symbol);
        BigDecimal indices = null;
        try {
            indices = TOKEN_INDICES_CACHE.get(key, () -> {
                GetIndicesReply getIndicesReply = grpcQuoteService.getIndices(symbol, orgId);
                if (Objects.isNull(getIndicesReply) || Objects.isNull(getIndicesReply.getIndicesMapMap())) {
                    throw new Exception("getIndices error");
                }
                Map<String, Index> map = getIndicesReply.getIndicesMapMap();
                if (map.get(symbol) == null) {
                    throw new Exception("getIndices error");
                }
                return new BigDecimal(map.get(symbol).getIndex().getStr());
            });
        } catch (Exception e) {
            log.warn("getIndices error orgId：{} tokenId：{}", orgId, symbol, e);
        }
        return indices;
    }

}
