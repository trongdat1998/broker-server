package io.bhex.broker.server.grpc.server.service;

import com.google.gson.reflect.TypeToken;
import io.bhex.base.token.GetSymbolMapReply;
import io.bhex.base.token.GetSymbolMapRequest;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.SymbolList;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.grpc.client.service.GrpcSymbolService;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.BrokerExchange;
import io.bhex.broker.server.model.Internationalization;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.InternationalizationMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BooleanUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 2019/10/12 11:14 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class SwapService {

    private final static Integer FUTURES_CATEGORY = 4;

    @Autowired
    private SymbolService symbolService;

    @Autowired
    private SymbolMapper symbolMapper;

    @Autowired
    private BrokerMapper brokerMapper;

    @Autowired
    private BasicService basicService;

    @Resource
    private GrpcSymbolService grpcSymbolService;

    @Resource
    private InternationalizationMapper internationalizationMapper;

    /**
     * 定时同步券商合作交易所的期权合约
     */
    @Scheduled(cron = "18 0/5 * * * ?")
    public void syncFuturesSymbol() {
        //获取全部券商
        List<Broker> brokerList = brokerMapper.selectAll();
        List<Long> brokerIds = brokerList.stream()
                .filter(broker -> broker.getStatus() == 1)
                .map(Broker::getOrgId).collect(Collectors.toList());
        for (Long brokerId : brokerIds) {

            // 获取交易所券商合作关系
            Map<Long, List<SymbolDetail>> exchangeFuturesMap = new HashMap<>();
            List<BrokerExchange> brokerExchangeList = basicService.listAllContract(brokerId);
            List<Long> exchangeIds = brokerExchangeList.stream().map(BrokerExchange::getExchangeId)
                    .distinct().collect(Collectors.toList());
            // 获取交易所全部币对信息
            GetSymbolMapRequest getSymbolMapRequest = GetSymbolMapRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(brokerId))
                    .addAllExchangeIds(exchangeIds).setForceRequest(true).build();
            GetSymbolMapReply getSymbolMapReply = grpcSymbolService.querySymbolList(getSymbolMapRequest);
            for (Long exchangeId : exchangeIds) {
                SymbolList symbolList = getSymbolMapReply.getSymbolMapMap().get(exchangeId);
                if (CollectionUtils.isEmpty(symbolList.getSymbolDetailsList())) {
                    log.warn("exchange:{} return null symbolList", exchangeId);
                    continue;
                }
                List<SymbolDetail> symbolDetails = symbolList.getSymbolDetailsList().stream().filter(s -> s.getCategory() == FUTURES_CATEGORY).collect(Collectors.toList());
                exchangeFuturesMap.put(exchangeId, symbolDetails);
            }

            // 获取本券商合作交易所列表，同步此交易所的全部可交易期货
            List<Long> eIds = brokerExchangeList.stream().filter(be -> be.getOrgId().equals(brokerId)).map(BrokerExchange::getExchangeId)
                    .distinct().collect(Collectors.toList());
            for (Long exchangeId : eIds) {
                List<SymbolDetail> exchangeFuturesList = exchangeFuturesMap.get(exchangeId);
                if (Objects.isNull(exchangeFuturesList)) {
                    continue;
                }
                List<Symbol> localSymbolList = getLocalSymbolList(brokerId, 0L, FUTURES_CATEGORY);
                Map<String, Symbol> symbolMap = localSymbolList.stream().collect(Collectors.toMap(Symbol::getSymbolId, Function.identity()));
                for (SymbolDetail symbolDetail : exchangeFuturesList) {
                    if (null != symbolDetail) {
                        if (StringUtils.isNotEmpty(symbolDetail.getSymbolNameLocaleJson())) {
                            List<SymbolNameLocale> nameLocaleList = JsonUtil.defaultGson().fromJson(symbolDetail.getSymbolNameLocaleJson(), new TypeToken<List<SymbolNameLocale>>() {
                            }.getType());
                            for (SymbolNameLocale snl : nameLocaleList) {
                                if (snl.getLocale().equals("en-us")) {
                                    snl.setLocale("en");
                                } else if (snl.getLocale().equals("zh-cn")) {
                                    snl.setLocale("zh");
                                }
                                Internationalization internationalization = internationalizationMapper.queryInternationalizationByKey(symbolDetail.getSymbolId(), snl.getLocale());
                                if (Objects.isNull(internationalization)) {
                                    Internationalization i = Internationalization.builder()
                                            .inKey(symbolDetail.getSymbolId())
                                            .env(snl.getLocale())
                                            .inValue(snl.getName())
                                            .type(0)
                                            .inStatus(0)
                                            .createAt(new Timestamp(System.currentTimeMillis()))
                                            .build();
                                    internationalizationMapper.insertSelective(i);
                                }
                            }
                        }
                        // 交易所允许交易期货与券商本地币对信息对比
                        if (Objects.isNull(symbolMap.get(symbolDetail.getSymbolId()))) {
                            Symbol s = symbolService.getBySymbolId(symbolDetail.getSymbolId(), brokerId);
                            if (null != s || symbolDetail.getAllowTrade() == false) {
                                continue;
                            }

                            try {
                                Symbol symbol = Symbol.builder()
                                        .orgId(brokerId)
                                        .exchangeId(symbolDetail.getExchangeId())
                                        .symbolId(symbolDetail.getSymbolId())
                                        .symbolName(symbolDetail.getSymbolName())
                                        .baseTokenId(symbolDetail.getBaseTokenId())
                                        .baseTokenName(symbolDetail.getBaseTokenId())
                                        .quoteTokenId(symbolDetail.getQuoteTokenId())
                                        .quoteTokenName(symbolDetail.getQuoteTokenId())
                                        .allowTrade(BooleanUtil.toInteger(Boolean.FALSE))
                                        .customOrder(0)
                                        .indexShow(0)
                                        .indexShowOrder(0)
                                        .needPreviewCheck(0)
                                        .openTime(0L)
                                        .created(System.currentTimeMillis())
                                        .updated(System.currentTimeMillis())
                                        .status(0)
                                        .category(new Long(symbolDetail.getCategory()).intValue())
                                        .build();
                                symbolService.addSymbol(symbol);
                            } catch (Exception e) {
                                log.warn("Sync Futures Symbol Error: insert symbol info error.", e);
                            }
                        }
                    }
                }
            }
        }
    }

    @Data
    public static class SymbolNameLocale {

        private String locale;

        private String name;
    }

    private List<Symbol> getLocalSymbolList(Long brokerId, Long exchangeId, Integer category) {
        Example example = Example.builder(Symbol.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", brokerId);
        criteria.andEqualTo("exchangeId", exchangeId);
        criteria.andEqualTo("category", category);
        return symbolMapper.selectByExample(example);
    }
}
