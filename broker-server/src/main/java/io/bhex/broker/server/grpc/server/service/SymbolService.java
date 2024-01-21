package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import io.bhex.base.account.GetOrgByIdReply;
import io.bhex.base.account.GetOrgByIdRequest;
import io.bhex.base.margin.QueryMarginSymbolReply;
import io.bhex.base.margin.QueryMarginSymbolRequest;
import io.bhex.base.margin.SetSymbolConfigRequest;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.token.GetSymbolRequest;
import io.bhex.base.token.GetTokenRequest;
import io.bhex.base.token.TokenCategory;
import io.bhex.base.token.TokenFuturesInfo;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.ExtraConfigUtil;
import io.bhex.broker.common.util.ExtraTagUtil;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.AdminSimplyReply;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.server.grpc.client.service.GrpcMarginService;
import io.bhex.broker.server.grpc.client.service.GrpcOrgService;
import io.bhex.broker.server.grpc.client.service.GrpcSymbolService;
import io.bhex.broker.server.grpc.client.service.GrpcTokenService;
import io.bhex.broker.server.model.BanSellConfig;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.SymbolFeeConfig;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.BooleanUtil;
import io.bhex.broker.server.util.PageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.Get;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 20/08/2018 9:55 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class SymbolService {

    @Autowired
    private SymbolMapper symbolMapper;
    @Autowired
    private TokenMapper tokenMapper;
    @Autowired
    private GrpcSymbolService grpcSymbolService;

    @Autowired
    private BanSellConfigMapper banSellConfigMapper;

    @Resource
    private SymbolFeeConfigMapper symbolFeeConfigMapper;

    @Resource
    private GrpcTokenService grpcTokenService;

    @Resource
    private BrokerFeeService brokerFeeService;
    @Resource
    private GrpcOrgService grpcOrgService;

    @Resource
    private GrpcMarginService grpcMarginService;

    public Integer countSymbol(Long brokerId, Long exchangeId, String quoteToken, List<String> symbolIdList, String symbolName, Integer category) {
        return symbolMapper.countByBrokerId(brokerId, exchangeId, quoteToken, symbolIdList, symbolName, category);
    }

    public Symbol getBySymbolId(String symbolId, Long brokerId) {
        return symbolMapper.getOrgSymbol(brokerId, symbolId);
    }

    public SymbolDetail queryOneSymbol(Long brokerId, String symbolId) {
        Symbol symbol = getBySymbolId(symbolId, brokerId);
        if (symbol == null) {
            return SymbolDetail.getDefaultInstance();
        }
        List<SymbolDetail> symbolDetails = symbolToGrpcDetail(Lists.newArrayList(symbol));
        return symbolDetails.get(0);
    }

    public QuerySymbolReply querySymbol(Integer current, Integer pageSize, String quoteToken,
                                        List<String> symbolIdList, String symbolName, Long brokerId, Long exchangeId, Integer category) {
        Integer total = countSymbol(brokerId, exchangeId, quoteToken, symbolIdList, symbolName, category);
        PageUtil.Page page = PageUtil.pageCount(current, pageSize, total);
        List<Symbol> symbols = symbolMapper.querySymbol(page.getStart(), page.getOffset(), brokerId, exchangeId,
                quoteToken, symbolIdList, symbolName, category);
        List<SymbolDetail> symbolDetails = symbolToGrpcDetail(symbols);
        QuerySymbolReply reply = QuerySymbolReply.newBuilder()
                .addAllSymbolDetails(symbolDetails)
                .setTotal(total)
                .setCurrent(current)
                .setPageSize(pageSize)
                .build();
        return reply;
    }

    public Boolean allowTrade(Long exchangeId, String symbolId, Boolean allowTrade, Long brokerId) {
        EditSymbolSwitchRequest request = EditSymbolSwitchRequest.newBuilder()
                .setBrokerId(brokerId)
                .setExchangeId(exchangeId)
                .setOpen(allowTrade)
                .setSymbolId(symbolId)
                .setSymbolSwitch(SymbolSwitchEnum.SYMBOL_TRADE_SWITCH)
                .setExtraInfo("")
                .build();
        return editSymbolSwitch(request).getSuccess();
        //return symbolMapper.allowTrade(exchangeId, symbolId, allowTrade ? 1 : 0, brokerId) > 0 ? true : false;
    }

    public Boolean updateBanSellStatus(Long exchangeId, String symbolId, Boolean banSellStatus, Long brokerId) {
        return symbolMapper.updateBanType(exchangeId, symbolId, banSellStatus ? 1 : 0, brokerId) > 0 ? true : false;
    }

    public Boolean updateWhiteAccountIdList(List<io.bhex.broker.grpc.admin.SaleWhite> accountIds, Long brokerId) {
        //清理白名单 insert 白名单
        if (accountIds != null && accountIds.size() > 0) {
            //清理白名单数据
            banSellConfigMapper.deleteByOrgId(brokerId);
            accountIds.stream().forEach(s -> {
                BanSellConfig banSellConfig = BanSellConfig
                        .builder()
                        .accountId(s.getAccountId())
                        .orgId(brokerId)
                        .userName(s.getUserName())
                        .updated(new Date().getTime())
                        .created(new Date().getTime())
                        .build();
                banSellConfigMapper.insert(banSellConfig);
            });
            return true;
        } else {
            return false;
        }
    }

    @Transactional
    public Boolean publish(Long exchangeId, String symbolId, Boolean isPublished, Long brokerId) {
        try {
            Symbol symbol = getBySymbolId(symbolId, brokerId);
            initBrokerTradeFeeConfig(symbol);
            symbol.setStatus(isPublished ? 1 : 0);
            symbol.setUpdated(System.currentTimeMillis());

            symbol.setBanSellStatus(1);
            symbol.setBanBuyStatus(1);
            symbol.setAllowTrade(0);
            if (symbol.getCategory() == TokenCategory.MAIN_CATEGORY_VALUE) {
                symbol.setShowStatus(0);
            }
            if (!isPublished) {//币对下架,杠杆改为未开通
                symbol.setAllowMargin(0);
            }


            symbolMapper.updateByPrimaryKeySelective(symbol);
            if (!isPublished) { //币对下架
                QueryMarginSymbolRequest marginSymbolRequest = QueryMarginSymbolRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(symbol.getOrgId()).build())
                        .setSymbolId(symbolId)
                        .build();
                QueryMarginSymbolReply marginSymbolReply = grpcMarginService.queryMarginSymbolReply(marginSymbolRequest);
                //为杠杆币对且开通交易，下架时同步关闭杠杆交易
                if (marginSymbolReply.getSymbolsCount() > 0 && marginSymbolReply.getSymbols(0).getAllowTrade() == 1) {
                    SetSymbolConfigRequest setSymbolConfigRequest = SetSymbolConfigRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(symbol.getOrgId()).build())
                            .setSymbolId(symbolId)
                            .setAllowTrade(2)
                            .build();
                    grpcMarginService.setSymbolConfig(setSymbolConfigRequest);
                }
            }
        } catch (Exception ex) {
            log.error("publish symbol add broker fee fail {}", ex);
            return false;
        }
        return true;
    }

    public Boolean addSymbol(Symbol symbol) {
        return symbolMapper.insertSelective(symbol) > 0 ? true : false;
    }

    public QueryExistSymbolReply queryBySymbolIds(Long exchangeId, Long brokerId, List<String> symbolIds) {
        QueryExistSymbolReply.Builder builder = QueryExistSymbolReply.newBuilder();

        if (CollectionUtils.isEmpty(symbolIds)) {
            return builder.build();
        }
        List<Symbol> symbols = symbolMapper.queryBySymbolIds(exchangeId, brokerId, symbolIds);

        builder.setTotal(symbols.size());
        List<SymbolDetail> symbolDetails = symbolToGrpcDetail(symbols);
        builder.addAllSymbolDetail(symbolDetails);

        return builder.build();
    }

    private List<SymbolDetail> symbolToGrpcDetail(List<Symbol> symbols) {
        List<SymbolDetail> result = new ArrayList<>();
        if (!CollectionUtils.isEmpty(symbols)) {
            for (Symbol s : symbols) {

                GetSymbolRequest getSymbolRequest = GetSymbolRequest.newBuilder()
                        .setBaseRequest(BaseReqUtil.getBaseRequest(s.getOrgId()))
                        .setSymbolId(s.getSymbolId())
                        .build();
                try {
                    io.bhex.base.token.SymbolDetail symbolDetail = grpcSymbolService.getSymbol(getSymbolRequest);
                    if (null != symbolDetail) {
                        SymbolDetail.Builder builder = SymbolDetail.newBuilder();
                        BeanCopyUtils.copyPropertiesIgnoreNull(s, builder);
                        builder.setExchangeId(s.getExchangeId());
                        builder.setMinPricePrecision(DecimalUtil.toTrimString(symbolDetail.getMinPricePrecision()));
                        builder.setMinTradeAmount(DecimalUtil.toTrimString(symbolDetail.getMinTradeAmount()));
                        builder.setMinTradeQuantity(DecimalUtil.toTrimString(symbolDetail.getMinTradeQuantity()));
                        builder.setBasePrecision(DecimalUtil.toTrimString(symbolDetail.getBasePrecision()));
                        builder.setAllowTrade(s.getAllowTrade() == 1);
                        builder.setPublished(s.getStatus() == 1);
                        builder.setBanSellStatus(s.getBanSellStatus() == 1);
                        builder.setBanBuyStatus(s.getBanBuyStatus() == 1);
                        //合约发布则显示
                        builder.setShowStatus(s.getCategory() == 4 ? s.getStatus() == 1 : s.getShowStatus() == 1);
                        builder.setFilterTopStatus(s.getFilterTopStatus() == 1);
                        builder.setDisplayTokenId(symbolDetail.getDisplayTokenId());
                        builder.setAllowMargin(s.getAllowMargin() == 1);
                        builder.setLabelId(s.getLabelId());
                        builder.setHideFromOpenapi(s.getHideFromOpenapi() == 1);
                        builder.setForbidOpenapiTrade(s.getForbidOpenapiTrade() == 1);
                        builder.putAllExtraTag(ExtraTagUtil.newInstance(s.getExtraTag()).map());
                        builder.putAllExtraConfig(ExtraConfigUtil.newInstance(s.getExtraConfig()).map());
                        builder.setAllowPlan(s.getAllowPlan() == 1);
                        builder.setIsAggregate(symbolDetail.getIsAggregate());
                        builder.setIsPrivate(symbolDetail.getIsPrivate());
                        builder.setApplyBrokerId(symbolDetail.getApplyBrokerId());
                        builder.setIsBaas(symbolDetail.getIsBaas());
                        builder.setIsTest(symbolDetail.getIsTest());
                        builder.setIsMainstream(symbolDetail.getIsMainstream());
                        result.add(builder.build());

                        if (s.getCategory().equals(4)) {
                            initBrokerTradeFeeConfig(s);
                        }
                    }
                } catch (BrokerException e) {
                    //调用 BH-server接口未查询到symbol信息则不显示本币对。
                    //调用方法已经处理异常，此处不中断
                    if (e.getCode() == BrokerErrorCode.GRPC_SERVER_TIMEOUT.code()) {
                        log.info(String.format("Get Symbol -> '%s' from BH Timeout.", s.getSymbolId()));
                    } else if (e.getCode() == BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR.code()) {
                        log.info(String.format("Symbol -> '%s' not exist in BH.", s.getSymbolId()));
                    }
                    continue;
                }
            }
        }
        return result;
    }

    public SymbolAgencyReply symbolAgency(SymbolAgencyRequest request) {
        for (String symbolId : request.getSymbolIdList()) {
            Symbol s = getBySymbolId(symbolId, request.getBrokerId());
            if (null != s) {
                continue;
            }

            GetSymbolRequest getSymbolRequest = GetSymbolRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(request.getBrokerId()))
                    .setSymbolId(symbolId)
                    .build();
            io.bhex.base.token.SymbolDetail symbolDetail = grpcSymbolService.getSymbol(getSymbolRequest);
            if (null != symbolDetail) {
                Symbol existedSymbol = symbolMapper.getBySymbolName(request.getBrokerId(), symbolDetail.getSymbolName());
                if (existedSymbol != null) {
                    return SymbolAgencyReply.newBuilder()
                            .setResult(false)
                            .setMessage("SymbolName " + symbolDetail.getSymbolName() + " existed")
                            .build();
                }

                String baseTokenName = "";
                String quoteTokenName = "";
                Token baseToken = tokenMapper.getToken(request.getBrokerId(), symbolDetail.getBaseTokenId());
                if (baseToken != null) {
                    baseTokenName = baseToken.getTokenName();
                } else { //如果券商还没上此币 则读平台tokenname
                    io.bhex.base.token.TokenDetail baseTokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder()
                            .setBaseRequest(BaseReqUtil.getBaseRequest(request.getBrokerId()))
                            .setTokenId(symbolDetail.getBaseTokenId()).build());
                    baseTokenName = baseTokenDetail.getTokenName();
                }

                Token quoteToken = tokenMapper.getToken(request.getBrokerId(), symbolDetail.getQuoteTokenId());
                if (quoteToken != null) {
                    quoteTokenName = quoteToken.getTokenName();
                } else {
                    io.bhex.base.token.TokenDetail quoteTokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder()
                            .setBaseRequest(BaseReqUtil.getBaseRequest(request.getBrokerId()))
                            .setTokenId(symbolDetail.getQuoteTokenId()).build());
                    quoteTokenName = quoteTokenDetail.getTokenName();
                }

                Symbol.SymbolBuilder symbolBuilder = Symbol.builder()
                        .orgId(request.getBrokerId())
                        .exchangeId(request.getExchangeId())
                        .symbolId(symbolDetail.getSymbolId())
                        .symbolName(baseTokenName + quoteTokenName)
                        .baseTokenId(symbolDetail.getBaseTokenId())
                        .baseTokenName(baseTokenName)
                        .quoteTokenId(symbolDetail.getQuoteTokenId())
                        .quoteTokenName(quoteTokenName)
                        .allowTrade(BooleanUtil.toInteger(Boolean.FALSE))
                        .showStatus(BooleanUtil.toInteger(Boolean.TRUE))
                        .banBuyStatus(BooleanUtil.toInteger(Boolean.FALSE))
                        .banSellStatus(BooleanUtil.toInteger(Boolean.FALSE))
                        .customOrder(0)
                        .indexShow(0)
                        .indexShowOrder(0)
                        .needPreviewCheck(0)
                        .openTime(0L)
                        .created(System.currentTimeMillis())
                        .updated(System.currentTimeMillis())
                        .status(0)
                        .category(new Long(symbolDetail.getCategory()).intValue());
                if (symbolDetail.getCategory() != TokenCategory.MAIN_CATEGORY_VALUE) {
                    symbolBuilder.symbolName(symbolDetail.getSymbolName());
                }

                Symbol symbol = symbolBuilder.build();
                log.info("{}", symbol);
                Boolean result = addSymbol(symbol);
                if (result) {
                    initBrokerTradeFeeConfig(symbol);
                }
            }
        }
        SymbolAgencyReply reply = SymbolAgencyReply.newBuilder()
                .setResult(true)
                .build();
        return reply;
    }

    private void initBrokerTradeFeeConfig(Symbol symbol) {
        SymbolFeeConfig symbolFeeConfig = brokerFeeService.querySymbolFeeConfigNoCache(symbol.getOrgId(), symbol.getExchangeId(), symbol.getSymbolId());
        Example example = new Example(Symbol.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", symbol.getOrgId());
        criteria.andEqualTo("exchangeId", symbol.getExchangeId());
        criteria.andEqualTo("symbolId", symbol.getSymbolId());
        if (symbolFeeConfig == null) {
            try {
                //币币 期权 期货 初始值费率不同 币币从平台获取 期权目前默认0.005 期货maker 0.0002 不同
                SymbolFeeConfig newSymbolFeeConfig = new SymbolFeeConfig();
                newSymbolFeeConfig.setOrgId(symbol.getOrgId());
                newSymbolFeeConfig.setStatus(1);
                newSymbolFeeConfig.setSymbolId(symbol.getSymbolId());
                newSymbolFeeConfig.setCategory(symbol.getCategory());
                newSymbolFeeConfig.setExchangeId(symbol.getExchangeId());
                newSymbolFeeConfig.setCreated(new Date());
                newSymbolFeeConfig.setUpdated(new Date());
                newSymbolFeeConfig.setBaseTokenId(symbol.getBaseTokenId());
                newSymbolFeeConfig.setBaseTokenName(symbol.getBaseTokenName());
                newSymbolFeeConfig.setQuoteTokenId(symbol.getQuoteTokenId());
                newSymbolFeeConfig.setQuoteTokenName(symbol.getQuoteTokenName());
                if (symbol.getCategory().equals(1)) {
                    //默认费率需要配置
                    newSymbolFeeConfig.setMakerBuyFee(new BigDecimal("0.001"));
                    newSymbolFeeConfig.setMakerSellFee(new BigDecimal("0.001"));
                    newSymbolFeeConfig.setTakerBuyFee(new BigDecimal("0.001"));
                    newSymbolFeeConfig.setTakerSellFee(new BigDecimal("0.001"));
                    this.symbolFeeConfigMapper.insertSelective(newSymbolFeeConfig);
                } else if (symbol.getCategory().equals(3)) {
                    //默认费率
                    newSymbolFeeConfig.setMakerBuyFee(new BigDecimal("0.005"));
                    newSymbolFeeConfig.setMakerSellFee(new BigDecimal("0.005"));
                    newSymbolFeeConfig.setTakerBuyFee(new BigDecimal("0.005"));
                    newSymbolFeeConfig.setTakerSellFee(new BigDecimal("0.005"));
                    this.symbolFeeConfigMapper.insertSelective(newSymbolFeeConfig);
                } else if (symbol.getCategory().equals(4)) {
                    //默认费率
                    newSymbolFeeConfig.setMakerBuyFee(new BigDecimal("0.0002"));
                    newSymbolFeeConfig.setMakerSellFee(new BigDecimal("0.0002"));
                    newSymbolFeeConfig.setTakerBuyFee(new BigDecimal("0.00075"));
                    newSymbolFeeConfig.setTakerSellFee(new BigDecimal("0.00075"));
                    this.symbolFeeConfigMapper.insertSelective(newSymbolFeeConfig);
                }
            } catch (Exception ex) {
                log.error("Init broker trade fee error {}", ex);
                throw ex;
            }
        }
    }


    public List<io.bhex.broker.grpc.admin.SaleWhite> queryWhiteAccountIdList(Long orgId) {
        List<BanSellConfig> banSellConfigs
                = this.banSellConfigMapper.queryAllByOrgId(orgId);
        if (banSellConfigs != null && banSellConfigs.size() > 0) {
            List<io.bhex.broker.grpc.admin.SaleWhite>
                    saleWhites = new ArrayList<>();
            banSellConfigs.stream().forEach(bs -> {
                saleWhites.add(io.bhex.broker.grpc.admin.SaleWhite
                        .newBuilder()
                        .setAccountId(bs.getAccountId())
                        .setUserName(bs.getUserName())
                        .build());
            });
            return saleWhites;
        } else {
            return new ArrayList<>();
        }
    }


    @Transactional
    public boolean editRecommendSymbols(long orgId, List<String> symbols) {
//        if (CollectionUtils.isEmpty(symbols)) {
//            return false;
//        }
        symbolMapper.resetRecommends(orgId);
        int size = symbols.size();
        for (int i = 0; i < size; i++) {
            symbolMapper.updateRecommendOrder(orgId, symbols.get(i), size - i);
        }
        return true;
    }

    public List<String> getRecommendSymbols(long orgId) {
        List<String> symbols = symbolMapper.getRecommendSymbols(orgId);
        if (CollectionUtils.isEmpty(symbols)) {
            return new ArrayList<>();
        }
        return symbols;
    }

    public AdminCommonResponse editSymbolSwitch(EditSymbolSwitchRequest request) {
        Symbol symbol = getBySymbolId(request.getSymbolId(), request.getBrokerId());
        int switchStatus = request.getOpen() ? 1 : 0;
        switch (request.getSymbolSwitch()) {
            case SYMBOL_BAN_SELL_SWITCH:
                symbol.setBanSellStatus(switchStatus);
                break;
            case SYMBOL_BAN_BUY_SWITCH:
                symbol.setBanBuyStatus(switchStatus);
                break;
            case SYMBOL_TRADE_SWITCH:
                symbol.setAllowTrade(switchStatus);
                symbol.setBanSellStatus(request.getOpen() ? 0 : 1);
                symbol.setBanBuyStatus(request.getOpen() ? 0 : 1);
                break;
            case SYMBOL_SHOW_SWITCH:
                symbol.setShowStatus(switchStatus);
                break;
            case SYMBOL_PUBLISH_SWITCH:
                symbol.setStatus(switchStatus);
                break;
            case SYMBOL_FILTER_TOP_SWITCH:
                symbol.setFilterTopStatus(switchStatus);
            case SYMBOL_PLAN_SWITCH:
                symbol.setAllowPlan(switchStatus);
            default:
                break;
        }

        //禁买 禁卖都存在的情况 交易就关闭
        if (symbol.getBanSellStatus() == 1 && symbol.getBanBuyStatus() == 1) {
            symbol.setAllowTrade(0);
        } else {
            symbol.setAllowTrade(1);
        }

        symbolMapper.updateByPrimaryKeySelective(symbol);
        return AdminCommonResponse.newBuilder().setSuccess(true).build();
    }


    @Transactional
    public boolean editQuoteSymbols(long orgId, int category, String quoteTokenId, List<String> symbols) {
        symbolMapper.resetQuoteSymbolCustomerOrder(orgId, quoteTokenId, category);
        int size = symbols.size();
        for (int i = 0; i < size; i++) {
            symbolMapper.updateQuoteSymbolCustomerOrder(orgId, quoteTokenId,
                    symbols.get(i).replace("/", ""), size - i, category);
        }
        return true;
    }

    public boolean editSymbolFilterTime(long orgId, String symbolId, long filterTime) {
        return symbolMapper.updateSymbolFilterTime(orgId, symbolId, filterTime) > 0;
    }

    public boolean setSymbolLabel(long orgId, String symbolId, long labelId) {
        return symbolMapper.setSymbolLabel(orgId, symbolId, labelId) == 1;
    }

    public boolean hideSymbolFromOpenapi(long orgId, String symbolId, boolean hideFromOpenapi) {
        return symbolMapper.hideSymbolFromOpenapi(orgId, symbolId, hideFromOpenapi ? 1 : 0) == 1;
    }

    public boolean forbidOpenapiTrade(long orgId, String symbolId, boolean forbidOpenapiTrade) {
        return symbolMapper.forbidOpenapiTrade(orgId, symbolId, forbidOpenapiTrade ? 1 : 0) == 1;
    }

    public List<String> getQuoteSymbols(long orgId, int category, String quoteTokenId) {
        List<Symbol> symbols = symbolMapper.getQuoteSymbols(orgId, quoteTokenId, category);
        if (CollectionUtils.isEmpty(symbols)) {
            return new ArrayList<>();
        }
        List<String> strings = symbols.stream()
                .map(s -> category == 1 ? (s.getBaseTokenId() + "/" + s.getQuoteTokenId()) : s.getSymbolId())
                .collect(Collectors.toList());
        return strings;
    }

    //下架已回收的券商币对
    public String removeUnusedBrokerSymbols(long orgId) {
        GetOrgByIdRequest idRequest = GetOrgByIdRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                .addId(orgId)
                .build();
        GetOrgByIdReply orgByIdReply = grpcOrgService.getOrgById(idRequest);
        if (orgByIdReply.getOrgConfigsCount() == 0) {
            return "no broker";
        }
        if (orgByIdReply.getOrgConfigs(0).getSaasEnable()) {
            return "broker in use";
        }
        int n = symbolMapper.removeBrokerSymbols(orgId);
        log.info("remove {} symbols", n);
        return "OK";
    }

    @Resource
    private FuturesBasicService futuresBasicService;

    public List<String> queryFuturesCoinToken(long orgId) {
        List<Symbol> orgSymbols = symbolMapper.queryOrgSymbols(orgId, Lists.newArrayList(TokenCategory.FUTURE_CATEGORY_VALUE));
        if (CollectionUtils.isEmpty(orgSymbols)) {
            return Lists.newArrayList();
        }
        List<String> orgSymbolIdList = orgSymbols
                .stream()
                .map(s -> s.getSymbolId())
                .collect(Collectors.toList());


        Map<String, TokenFuturesInfo> tokenMap = futuresBasicService.getTokenFuturesMap(orgId);
        if (CollectionUtils.isEmpty(tokenMap)) {
            return Lists.newArrayList();
        }
        List<String> result = Lists.newArrayList();
        for (String symbol : tokenMap.keySet()) {
            TokenFuturesInfo info = tokenMap.get(symbol);
            log.info("{} {} {} {}", symbol, !orgSymbolIdList.contains(symbol), StringUtils.isEmpty(info.getCoinToken()), info.getIssueDate() <= System.currentTimeMillis());
            if (!orgSymbolIdList.contains(symbol) || StringUtils.isEmpty(info.getCoinToken())) {
                continue;
            }
            result.add(info.getCoinToken());
        }
        log.info("{}", orgSymbolIdList);

        return result.stream().distinct().collect(Collectors.toList());
    }

    public AdminSimplyReply editSymbolExtraTags(long orgId, String symbolId, Map<String, Integer> tagMap) {
        Symbol symbol = symbolMapper.getOrgSymbol(orgId, symbolId);
        if (symbol == null) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        String newTagJson = ExtraTagUtil.newInstance(symbol.getExtraTag()).putAll(tagMap).jsonStr();
        symbol.setExtraTag(newTagJson);
        symbol.setUpdated(System.currentTimeMillis());
        symbolMapper.updateByPrimaryKeySelective(symbol);
        return AdminSimplyReply.newBuilder().setResult(true).build();
    }

    public AdminSimplyReply editSymbolExtraConfigs(long orgId, String symbolId, Map<String, String> configMap) {
        Symbol symbol = symbolMapper.getOrgSymbol(orgId, symbolId);
        if (symbol == null) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        String newTagJson = ExtraConfigUtil.newInstance(symbol.getExtraConfig()).putAll(configMap).jsonStr();
        symbol.setExtraConfig(newTagJson);
        symbol.setUpdated(System.currentTimeMillis());
        symbolMapper.updateByPrimaryKeySelective(symbol);
        return AdminSimplyReply.newBuilder().setResult(true).build();
    }


}
