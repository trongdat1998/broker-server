package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.bhex.base.proto.UnderlyingTypeEnum;
import io.bhex.base.token.*;
import io.bhex.broker.grpc.basic.Underlying;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.grpc.client.service.GrpcSymbolService;
import io.bhex.broker.server.grpc.client.service.GrpcTokenService;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.BrokerExchange;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.FuturesUtil;
import io.bhex.broker.server.util.GrpcRequestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FuturesBasicService {

    @Resource
    SymbolMapper symbolMapper;

    @Resource
    GrpcTokenService grpcTokenService;

    @Resource
    GrpcSymbolService grpcSymbolService;

    @Resource
    BasicService basicService;

    @Resource
    private BrokerMapper brokerMapper;


    /**
     * Query futures symbols
     *
     * @return symbols
     */
    public List<io.bhex.broker.grpc.basic.Symbol> queryFuturesSymbols(Header header) {
        List<SymbolDetail> symbolDetails = getExchangeSymbols(header);//期货symbol list
        Map<String, TokenFuturesInfo> tokenFuturesMap = getTokenFuturesMap(header.getOrgId());//期货token list
        Map<String, List<io.bhex.broker.server.model.Symbol>> underlyingDetailsMap = getBrokerUnderlyingDetailMap();

        List<io.bhex.broker.grpc.basic.Symbol> symbols = new ArrayList<>();
        for (SymbolDetail symbolDetail : symbolDetails) {
            if (!FuturesUtil.validSymbolDetail(symbolDetail)) {
                continue;
            }

            String displayUnderlyingId = symbolDetail.getDisplayUnderlyingId();
            String underlyingDetailKey = String.format("%s_%s",
                    symbolDetail.getExchangeId(), symbolDetail.getSymbolId());
            List<io.bhex.broker.server.model.Symbol> underlyingDetails = underlyingDetailsMap.get(underlyingDetailKey);
            if (CollectionUtils.isEmpty(underlyingDetails)) {
                continue;
            }

            String baseTokenId = symbolDetail.getBaseTokenId();
            TokenFuturesInfo baseTokenFutures = tokenFuturesMap.get(baseTokenId);
            if (baseTokenFutures == null) {
                continue;
            }

            for (io.bhex.broker.server.model.Symbol underlyingDetail : underlyingDetails) {
                io.bhex.broker.grpc.basic.Symbol symbol = FuturesUtil.toFuturesSymbol(
                        underlyingDetail, symbolDetail, baseTokenFutures);
                // FirstLevelUnderlyingId和SecondLevelUnderlyingId只作为前端多级分区展示
                // 因此这里SecondLevelUnderlyingId的值应该设置为displayUnderlyingId
                // TODO: 前端多级分区展示需要在broker端重构，不依赖平台。这里临时使用tag标签来做临时处理，解决同一个Underlying对应的合约无法区分正向反向的分组的问题
                Underlying underlying = BasicService.underlyingMap.get(displayUnderlyingId);
                if (underlying != null) {
                    if (StringUtils.isNotEmpty(underlying.getTag())) {
                        // 如果tag不为空，parentUnderlyingId存储一级反向分组ID tag存储一级正向分组ID
                        symbol = symbol.toBuilder()
                                .setFirstLevelUnderlyingId(symbolDetail.getIsReverse() ? underlying.getParentUnderlyingId() : underlying.getTag())
                                .setFirstLevelUnderlyingName(symbolDetail.getIsReverse() ? underlying.getParentUnderlyingId() : underlying.getTag())
                                .setSecondLevelUnderlyingId(displayUnderlyingId)
                                .setSecondLevelUnderlyingName(displayUnderlyingId)
                                .build();
                    } else {
                        symbol = symbol.toBuilder()
                                .setFirstLevelUnderlyingId(underlying.getParentUnderlyingId())
                                .setFirstLevelUnderlyingName(underlying.getParentUnderlyingId())
                                .setSecondLevelUnderlyingId(displayUnderlyingId)
                                .setSecondLevelUnderlyingName(displayUnderlyingId)
                                .build();
                    }
                }
                symbols.add(symbol);
            }
        }

        return symbols;
    }

    /**
     * Init futures symbols cache
     */
    public void initFuturesSymbols(Long orgId) {
        List<SymbolDetail> symbols = getExchangeSymbols();//期货symbol list
        Map<String, TokenFuturesInfo> tokenFuturesMap = getTokenFuturesMap(orgId);//期货token list
        Map<String, List<io.bhex.broker.server.model.Symbol>> underlyingDetailsMap = getBrokerUnderlyingDetailMap();

        symbols = Optional.ofNullable(symbols).orElse(new ArrayList<>())
                .stream()
                .filter(FuturesUtil::validSymbolDetail)
                .map(symbolDetail -> {
                    String underlyingDetailKey = String.format("%s_%s",
                            symbolDetail.getExchangeId(), symbolDetail.getSymbolId());
                    List<io.bhex.broker.server.model.Symbol> underlyingDetails = underlyingDetailsMap.get(underlyingDetailKey);
                    if (CollectionUtils.isEmpty(underlyingDetails)) {
                        return null;
                    }
                    String baseTokenId = symbolDetail.getBaseTokenId();
                    TokenFuturesInfo baseTokenFutures = tokenFuturesMap.get(baseTokenId);
                    if (baseTokenFutures == null) {
                        return null;
                    }
                    return symbolDetail;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        Map<String, List<SymbolDetail>> futureSymbolByQuoteTokenIdMap = symbols.stream()
                .collect(Collectors.groupingBy(SymbolDetail::getQuoteTokenId));
        BasicService.futureSymbolByQuoteTokenIdMap = ImmutableMap.copyOf(futureSymbolByQuoteTokenIdMap);

        Map<String, SymbolDetail> exchangeSymbolDetailFuturesMap = new HashMap<>();
        for (SymbolDetail symbol : symbols) {
            exchangeSymbolDetailFuturesMap.put(String.format("%s_%s", symbol.getExchangeId(), symbol.getSymbolId()), symbol);
        }

        BasicService.exchangeSymbolDetailFuturesMap = ImmutableMap.copyOf(exchangeSymbolDetailFuturesMap);
        Map<String, Long> orgExchangeFuturesMap = new HashMap<>();
        for (SymbolDetail symbolDetail : symbols) {
            String underlyingDetailKey = String.format("%s_%s",
                    symbolDetail.getExchangeId(), symbolDetail.getSymbolId());
            List<io.bhex.broker.server.model.Symbol> underlyingDetails = underlyingDetailsMap.get(underlyingDetailKey);
            if (CollectionUtils.isEmpty(underlyingDetails)) {
                continue;
            }

            for (io.bhex.broker.server.model.Symbol underlyingDetail : underlyingDetails) {
                String key = String.format("%s_%s", underlyingDetail.getOrgId(), symbolDetail.getSymbolId());
                orgExchangeFuturesMap.put(key, underlyingDetail.getExchangeId());
            }
        }
        log.info("init orgExchangeFuturesMap:{}", orgExchangeFuturesMap);
        BasicService.orgExchangeFuturesMap = ImmutableMap.copyOf(orgExchangeFuturesMap);

        Map<String, io.bhex.broker.server.model.Symbol> brokerOrgFuturesSymbolMap = new HashMap<>();
        List<Integer> categories = Collections.singletonList(TokenCategory.FUTURE_CATEGORY.getNumber());
        List<io.bhex.broker.server.model.Symbol> brokerSymbols = symbolMapper.queryAll(categories);
        for (io.bhex.broker.server.model.Symbol symbol : brokerSymbols) {
            String key = String.format("%s_%s", symbol.getOrgId(), symbol.getSymbolId());
            brokerOrgFuturesSymbolMap.put(key, symbol);
        }
        BasicService.brokerOrgFuturesSymbolMap = ImmutableMap.copyOf(brokerOrgFuturesSymbolMap);
    }

    /**
     * Init futures underlying cache
     */
    public void initFuturesUnderlying(Long orgId) {
        Map<String, Underlying> underlyingMap = basicService.getUnderlyings(UnderlyingTypeEnum.UNDERLYING_TYPE_FUTURE.getNumber(), orgId)
                .stream()
                .collect(Collectors.toMap(Underlying::getUnderlyingId, underlying -> underlying, (p, q) -> p));
        BasicService.underlyingMap = ImmutableMap.copyOf(underlyingMap);
    }

    private Map<String, List<io.bhex.broker.server.model.Symbol>> getBrokerUnderlyingDetailMap() {
        List<Integer> categories = Collections.singletonList(TokenCategory.FUTURE_CATEGORY.getNumber());
        // 获取可用的期货标的列表
        List<io.bhex.broker.server.model.Symbol> underlyingDetails = symbolMapper.queryAll(categories);
        if (CollectionUtils.isEmpty(underlyingDetails)) {
            return new HashMap<>();
        }

        return underlyingDetails.stream().collect(
                Collectors.groupingBy(s -> String.format("%s_%s", s.getExchangeId(), s.getBaseTokenId())));
    }

    public Map<String, TokenFuturesInfo> getTokenFuturesMap(Long orgId) {
        GetTokenListRequest request = GetTokenListRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .addAllTypes(Collections.singletonList(TokenTypeEnum.FUTURE))
                .setForceRequest(true)
                .build();
        TokenList tokenList = grpcTokenService.queryTokenList(request);

        if (tokenList == null || CollectionUtils.isEmpty(tokenList.getTokenDetailsList())) {
            return new HashMap<>();
        }
        List<TokenDetail> details = Optional.ofNullable(tokenList.getTokenDetailsList()).orElse(new ArrayList<>());
        return ImmutableMap.copyOf(details
                .stream()
                .map(TokenDetail::getTokenFuturesInfo)
                .collect(
                        Collectors.toMap(TokenFuturesInfo::getTokenId, tokenFutures -> tokenFutures, (p, q) -> p)
                )
        );
    }

    /*
     * 这里只加载的是合约的交易币对信息！！！
     */
    private List<SymbolDetail> getExchangeSymbols() {
        List<Broker> brokerList = brokerMapper.queryAvailableBroker();
        List<SymbolDetail> symbolDetailList = Lists.newArrayList();
        for (Broker broker : brokerList) {
            if (BrokerService.checkModule(broker.getOrgId(), FunctionModule.FUTURES)) {
                List<SymbolDetail> symbolDetails = getExchangeSymbols(Header.newBuilder().setOrgId(broker.getOrgId()).build());
                //使用单个broker
//                GetMakerBonusConfigReply reply = grpcSymbolService.querySymbolMakerBonusConfig(GetMakerBonusConfigRequest.newBuilder()
//                        .addBrokerId(broker.getOrgId()).build(), broker.getOrgId());
//                List<MakerBonusConfig> makerBonusConfigLists = reply.getBonusMapMap().get(broker.getOrgId()).getConfigsList();
//                Map<String, MakerBonusConfig> makerBonusConfigMap = makerBonusConfigLists.stream()
//                        .collect(Collectors.toMap(config -> Strings.nullToEmpty(config.getSymbolId()).trim(), item -> item, (p, q) -> q));
//                for (SymbolDetail symbolDetail : symbolDetails) {
//                    if (makerBonusConfigMap.get(symbolDetail.getSymbolId()) != null) {
//                        symbolDetail = symbolDetail.toBuilder()
//                                .setMakerBonusRate(DecimalUtil.fromBigDecimal(new BigDecimal(makerBonusConfigMap.get(symbolDetail.getSymbolId()).getMakerBonusRate())))
//                                .setMinInterestFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal(makerBonusConfigMap.get(symbolDetail.getSymbolId()).getMinInterestFeeRate())))
//                                .setMinTakerFeeRate(DecimalUtil.fromBigDecimal(new BigDecimal(makerBonusConfigMap.get(symbolDetail.getSymbolId()).getMinTakerFeeRate())))
//                                .build();
//                    } else {
//                        symbolDetail = symbolDetail.toBuilder()
//                                .setMakerBonusRate(DecimalUtil.fromBigDecimal(BigDecimal.ZERO))
//                                .setMinInterestFeeRate(DecimalUtil.fromBigDecimal(BigDecimal.ZERO))
//                                .setMinTakerFeeRate(DecimalUtil.fromBigDecimal(BigDecimal.ZERO))
//                                .build();
//                    }
//                    symbolDetailList.add(symbolDetail);
//                }
                symbolDetailList.addAll(symbolDetails);
            }
        }
        return symbolDetailList;
    }

    /**
     * 根据SAAS平台合约配置来获取所有交易所币对信息
     */
    private List<SymbolDetail> getExchangeSymbols(Header header) {
        List<BrokerExchange> brokerExchangeList = null;
        if (header.getOrgId() != 0) {
            brokerExchangeList = basicService.listAllContract(header.getOrgId());
        } else {
            brokerExchangeList = basicService.listAllContract();
        }
        List<Long> exchangeIds = CollectionUtils.isEmpty(brokerExchangeList) ? Lists.newArrayList(301L) : brokerExchangeList.stream()
                .map(BrokerExchange::getExchangeId)
                .distinct()
                .collect(Collectors.toList());//期货 exchange id list

        if (CollectionUtils.isEmpty(exchangeIds)) {
            return new ArrayList<>();
        }
        GetSymbolMapRequest request = GetSymbolMapRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .addAllExchangeIds(exchangeIds).setForceRequest(true).build();
        GetSymbolMapReply reply = grpcSymbolService.querySymbolList(request);

        List<SymbolDetail> list = new ArrayList<>();
        for (Long exchangeId : exchangeIds) {
            SymbolList symbolList = reply.getSymbolMapMap().get(exchangeId);
            if (symbolList == null) {
                log.warn("getSymbolDetails exchange:{} return null symbolList", exchangeId);
                continue;
            }
            List<SymbolDetail> symbolDetails = symbolList.getSymbolDetailsList();
            if (CollectionUtils.isEmpty(symbolDetails)) {
                log.warn("getSymbolDetails exchange:{} return null symbolList", exchangeId);
                continue;
            }
            list.addAll(symbolDetails);
        }
        return list;
    }

    public void initFuturesTokens(Long orgId) {
        BasicService.tokenFuturesInfoMap = ImmutableMap.copyOf(getTokenFuturesMap(orgId));
    }
}